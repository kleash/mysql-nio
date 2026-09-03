import NIOCore

extension MySQLDatabase {
    public func simpleQuery(_ sql: String) -> EventLoopFuture<[MySQLRow]> {
        nonisolated(unsafe) var rows = [MySQLRow]()
        return self.simpleQuery(sql) { row in
            rows.append(row)
        }.map { rows }
    }
    
    /// Result sets, each carrying its own columns. A `CALL` to a stored
    /// procedure can return several, and a caller can't recover the
    /// boundaries from the rows alone — two result sets may have identical
    /// column shapes — so they have to be reported while reading.
    ///
    /// A result set is recorded as soon as the server announces its
    /// columns, so a `SELECT` that matched nothing still comes back (with
    /// its columns and no rows) instead of vanishing. Statements that
    /// return no result set at all — `INSERT`, `USE`, DDL — never announce
    /// columns and so contribute no entry.
    public func simpleQueryResultSets(_ sql: String) -> EventLoopFuture<[MySQLResultSet]> {
        nonisolated(unsafe) var resultSets: [MySQLResultSet] = []
        nonisolated(unsafe) var columns: [MySQLProtocol.ColumnDefinition41] = []
        nonisolated(unsafe) var rows: [MySQLRow] = []
        return self.simpleQuery(sql, onRow: { row in
            rows.append(row)
        }, onResultSetBoundary: {
            resultSets.append(.init(columns: columns, rows: rows))
            columns = []
            rows = []
        }, onResultSetColumns: { announced in
            columns = announced
        }).map {
            // The last result set has no boundary after it — the command
            // finishing is what closes it.
            if !columns.isEmpty || !rows.isEmpty {
                resultSets.append(.init(columns: columns, rows: rows))
            }
            return resultSets
        }
    }

    public func simpleQuery(_ sql: String, onRow: @escaping (MySQLRow) -> ()) -> EventLoopFuture<Void> {
        self.simpleQuery(sql, onRow: onRow, onResultSetBoundary: {})
    }

    public func simpleQuery(
        _ sql: String,
        onRow: @escaping (MySQLRow) -> (),
        onResultSetBoundary: @escaping () -> (),
        onResultSetColumns: @escaping ([MySQLProtocol.ColumnDefinition41]) -> () = { _ in }
    ) -> EventLoopFuture<Void> {
        let query = MySQLSimpleQueryCommand(
            sql: sql,
            onRow: onRow,
            onResultSetBoundary: onResultSetBoundary,
            onResultSetColumns: onResultSetColumns
        )
        return self.send(query, logger: self.logger)
    }
}

private final class MySQLSimpleQueryCommand: MySQLCommand, @unchecked Sendable { // this is cheating
    let sql: String
    
    enum State {
        case ready
        case columns(count: UInt64)
        case rows
        case done
    }
    var state: State
    
    var columns: [MySQLProtocol.ColumnDefinition41]
    let onRow: (MySQLRow) -> ()
    let onResultSetBoundary: () -> ()
    let onResultSetColumns: ([MySQLProtocol.ColumnDefinition41]) -> ()

    init(
        sql: String,
        onRow: @escaping (MySQLRow) -> (),
        onResultSetBoundary: @escaping () -> () = {},
        onResultSetColumns: @escaping ([MySQLProtocol.ColumnDefinition41]) -> () = { _ in }
    ) {
        self.state = .ready
        self.sql = sql
        self.columns = []
        self.onRow = onRow
        self.onResultSetBoundary = onResultSetBoundary
        self.onResultSetColumns = onResultSetColumns
    }
    
    func handle(packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        // print("QUERY \(state): \(packet.payload.debugDescription)")
        guard !packet.isError else {
            self.state = .done
            let errorPacket = try packet.decode(MySQLProtocol.ERR_Packet.self, capabilities: capabilities)
            let error: any Error
            switch errorPacket.errorCode {
            case .DUP_ENTRY:
                error = MySQLError.duplicateEntry(errorPacket.errorMessage)
            case .PARSE_ERROR:
                error = MySQLError.invalidSyntax(errorPacket.errorMessage)
            default:
                error = MySQLError.server(errorPacket)
            }
            throw error
        }
        switch self.state {
        case .ready:
            if packet.isOK {
                self.state = .done
                return .done
            } else {
                let res = try packet.decode(MySQLProtocol.COM_QUERY_Response.self, capabilities: capabilities)
                self.state = .columns(count: res.columnCount)
                return .noResponse
            }
        case .columns(let total):
            let column = try packet.decode(MySQLProtocol.ColumnDefinition41.self, capabilities: capabilities)
            self.columns.append(column)
            if self.columns.count == numericCast(total) {
                // The server has finished describing this result set.
                // Reporting it here — before any row arrives — is what lets
                // a caller record a result set that turns out to have no
                // rows at all.
                self.onResultSetColumns(self.columns)
                self.state = .rows
            }
            return .noResponse
        case .rows:
            if packet.isEOF {
                // With CLIENT_MULTI_RESULTS a `CALL` that returns result
                // sets terminates each one with SERVER_MORE_RESULTS_EXISTS
                // set; without consuming those follow-up result sets the
                // connection desyncs (the next command hangs waiting for a
                // reply that's already been overtaken by leftover packets).
                var terminator = packet
                let hasMoreResults = (try? MySQLProtocol.OK_Packet.decode(from: &terminator, capabilities: capabilities))?
                    .statusFlags.contains(.SERVER_MORE_RESULTS_EXISTS) ?? false
                if hasMoreResults {
                    self.onResultSetBoundary()
                    self.columns = []
                    self.state = .ready
                    return .noResponse
                }
                self.state = .done
                return .done
            }
            
            let data = try MySQLProtocol.TextResultSetRow.decode(from: &packet, columnCount: columns.count)
            let row = MySQLRow(
                format: .text,
                columnDefinitions: self.columns,
                values: data.values
            )
            self.onRow(row)
            return .noResponse
        case .done: throw MySQLError.protocolError
        }
    }
    
    func activate(capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        try .response([
            .encode(MySQLProtocol.COM_QUERY(query: self.sql), capabilities: capabilities)
        ])
    }
}
