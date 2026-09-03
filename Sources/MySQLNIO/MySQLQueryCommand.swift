import NIOCore
import Logging

public struct MySQLQueryMetadata {
    /// `int<lenenc>`    `affected_rows`    affected rows
    public let affectedRows: UInt64
    
    /// `int<lenenc>`    `last_insert_id`    last insert-id
    public let lastInsertID: UInt64?
}

extension MySQLDatabase {
    public func query(
        _ sql: String,
        _ binds: [MySQLData] = [],
        onMetadata: @escaping (MySQLQueryMetadata) throws -> () = { _ in }
    ) -> EventLoopFuture<[MySQLRow]> {
        nonisolated(unsafe) var rows = [MySQLRow]()
        return self.query(sql, binds, onRow: { row in
            rows.append(row)
        }, onMetadata: onMetadata).map { rows }
    }
    
    /// Result sets, each carrying its own columns — see
    /// `simpleQueryResultSets(_:)` for why the boundaries have to be
    /// reported while reading rather than inferred afterwards.
    ///
    /// A result set is recorded as soon as the server announces its
    /// columns, so a `SELECT` that matched nothing still comes back (with
    /// its columns and no rows) instead of vanishing. Statements that
    /// return no result set at all — `INSERT`, `USE`, DDL — never announce
    /// columns and so contribute no entry.
    public func queryResultSets(
        _ sql: String,
        _ binds: [MySQLData] = [],
        onMetadata: @escaping (MySQLQueryMetadata) throws -> () = { _ in }
    ) -> EventLoopFuture<[MySQLResultSet]> {
        nonisolated(unsafe) var resultSets: [MySQLResultSet] = []
        nonisolated(unsafe) var columns: [MySQLProtocol.ColumnDefinition41] = []
        nonisolated(unsafe) var rows: [MySQLRow] = []
        return self.query(sql, binds, onRow: { row in
            rows.append(row)
        }, onMetadata: onMetadata, onResultSetBoundary: {
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

    public func query(
        _ sql: String,
        _ binds: [MySQLData] = [],
        onRow: @escaping (MySQLRow) throws -> (),
        onMetadata: @escaping (MySQLQueryMetadata) throws -> () = { _ in }
    ) -> EventLoopFuture<Void> {
        self.query(sql, binds, onRow: onRow, onMetadata: onMetadata, onResultSetBoundary: {})
    }

    public func query(
        _ sql: String,
        _ binds: [MySQLData] = [],
        onRow: @escaping (MySQLRow) throws -> (),
        onMetadata: @escaping (MySQLQueryMetadata) throws -> () = { _ in },
        onResultSetBoundary: @escaping () -> (),
        onResultSetColumns: @escaping ([MySQLProtocol.ColumnDefinition41]) -> () = { _ in }
    ) -> EventLoopFuture<Void> {
        let query = MySQLQueryCommand(
            sql: sql,
            binds: binds,
            onRow: onRow,
            onMetadata: onMetadata,
            onResultSetBoundary: onResultSetBoundary,
            onResultSetColumns: onResultSetColumns,
            logger: self.logger
        )
        return self.send(query, logger: self.logger)
    }
}

private final class MySQLQueryCommand: MySQLCommand, @unchecked Sendable { // this is cheating
    let sql: String
    
    enum State {
        case ready
        case params
        case columns
        case executeColumnCount
        case executeColumns(remaining: Int)
        case rows
        case done
    }

    var state: State
    let binds: [MySQLData]
    let onRow: (MySQLRow) throws -> ()
    let onMetadata: (MySQLQueryMetadata) throws -> ()
    let onResultSetBoundary: () -> ()
    let onResultSetColumns: ([MySQLProtocol.ColumnDefinition41]) -> ()
    let logger: Logger

    private var prepareColumns: [MySQLProtocol.ColumnDefinition41]
    private var executeColumns: [MySQLProtocol.ColumnDefinition41]
    private var params: [MySQLProtocol.ColumnDefinition41]
    private var ok: MySQLProtocol.COM_STMT_PREPARE_OK?

    private var lastUserError: (any Error)?
    var statementID: UInt32?
    
    init(
        sql: String,
        binds: [MySQLData],
        onRow: @escaping (MySQLRow) throws -> (),
        onMetadata: @escaping (MySQLQueryMetadata) throws -> (),
        onResultSetBoundary: @escaping () -> () = {},
        onResultSetColumns: @escaping ([MySQLProtocol.ColumnDefinition41]) -> () = { _ in },
        logger: Logger
    ) {
        self.state = .ready
        self.sql = sql
        self.binds = binds
        self.prepareColumns = []
        self.executeColumns = []
        self.params = []
        self.onRow = onRow
        self.onMetadata = onMetadata
        self.onResultSetBoundary = onResultSetBoundary
        self.onResultSetColumns = onResultSetColumns
        self.logger = logger
    }
    
    func handle(packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        self.logger.trace("MySQLQueryCommand.\(self.state)")
        guard !packet.isError else {
            self.state = .done

            let errorPacket = try packet.decode(
                MySQLProtocol.ERR_Packet.self,
                capabilities: capabilities
            )
            let error: any Error
            switch errorPacket.errorCode {
            case .DUP_ENTRY:
                error = MySQLError.duplicateEntry(errorPacket.errorMessage)
            case .PARSE_ERROR:
                error = MySQLError.invalidSyntax(errorPacket.errorMessage)
            default:
                error = MySQLError.server(errorPacket)
            }

            var response: [MySQLPacket] = []
            if let statementID = self.statementID {
                self.statementID = nil
                var packet = MySQLPacket()
                MySQLProtocol.COM_STMT_CLOSE(
                    statementID: statementID
                ).encode(into: &packet)
                response = [packet]
            }

            return .init(
                response: response,
                done: true,
                resetSequence: true,
                error: error
            )
        }
        switch self.state {
        case .ready:
            let res = try packet.decode(MySQLProtocol.COM_STMT_PREPARE_OK.self, capabilities: capabilities)
            self.ok = res
            if res.numParams != 0 {
                self.state = .params
            } else if res.numColumns != 0 {
                self.state = .columns
            } else {
                self.state = .executeColumnCount
            }
            self.statementID = res.statementID
            let execute = MySQLProtocol.COM_STMT_EXECUTE(
                statementID: res.statementID,
                flags: [],
                values: self.binds
            )
            return try .init(response: [.encode(execute, capabilities: capabilities)], resetSequence: true)
        case .params:
            let param = try packet.decode(MySQLProtocol.ColumnDefinition41.self, capabilities: capabilities)
            self.params.append(param)
            if self.params.count == numericCast(self.ok!.numParams) {
                if self.ok!.numColumns != 0 {
                    self.state = .columns
                } else {
                    self.state = .rows
                }
            }
            return .noResponse
        case .columns:
            let column = try packet.decode(MySQLProtocol.ColumnDefinition41.self, capabilities: capabilities)
            self.prepareColumns.append(column)
            if self.prepareColumns.count == numericCast(self.ok!.numColumns) {
                self.state = .executeColumnCount
            }
            return .noResponse
        case .executeColumnCount:
            guard !packet.isOK else {
                return try self.done(packet: &packet, capabilities: capabilities)
            }
            guard let count = packet.payload.readLengthEncodedInteger() else {
                throw MySQLError.protocolError
            }
            self.state = .executeColumns(remaining: numericCast(count))
            return .noResponse
        case .executeColumns(var remaining):
            let column = try packet.decode(MySQLProtocol.ColumnDefinition41.self, capabilities: capabilities)
            self.executeColumns.append(column)
            remaining -= 1
            switch remaining {
            case 0:
                // The server has finished describing this result set.
                // Reporting it here — before any row arrives — is what lets
                // a caller record a result set that turns out to have no
                // rows at all.
                self.onResultSetColumns(self.executeColumns)
                self.state = .rows
            default:
                self.state = .executeColumns(remaining: remaining)
            }
            return .noResponse
        case .rows:
            if packet.isEOF || packet.isOK && executeColumns.count == 0 {
                // See the matching comment in `MySQLSimpleQueryCommand`:
                // a result-set-returning `CALL` sends several result sets
                // back to back, and `done(packet:capabilities:)` would both
                // stop reading early *and* send COM_STMT_CLOSE while those
                // packets are still in flight.
                var terminator = packet
                let hasMoreResults = (try? MySQLProtocol.OK_Packet.decode(from: &terminator, capabilities: capabilities))?
                    .statusFlags.contains(.SERVER_MORE_RESULTS_EXISTS) ?? false
                if hasMoreResults {
                    self.onResultSetBoundary()
                    self.executeColumns = []
                    self.state = .executeColumnCount
                    return .noResponse
                }
                return try self.done(packet: &packet, capabilities: capabilities)
            }

            let data = try MySQLProtocol.BinaryResultSetRow.decode(from: &packet, columns: executeColumns)
            let row = MySQLRow(
                format: .binary,
                columnDefinitions: self.executeColumns,
                values: data.values
            )
            do {
                try self.onRow(row)
            } catch {
                self.lastUserError = error
            }
            return .noResponse
        case .done: fatalError()
        }
    }

    func done(packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        self.state = .done
        if packet.isOK {
            let ok = try MySQLProtocol.OK_Packet.decode(from: &packet, capabilities: capabilities)
            do {
                try self.onMetadata(.init(affectedRows: ok.affectedRows, lastInsertID: ok.lastInsertID))
            } catch {
                self.lastUserError = error
            }
        }
        var packet = MySQLPacket()
        MySQLProtocol.COM_STMT_CLOSE(
            statementID: self.ok!.statementID
        ).encode(into: &packet)
        self.statementID = nil
        return .init(
            response: [packet],
            done: true,
            resetSequence: true,
            error: self.lastUserError
        )
    }
    
    func activate(capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        let prepare = MySQLProtocol.COM_STMT_PREPARE(query: self.sql)
        return try .response([.encode(prepare, capabilities: capabilities)])
    }

    deinit {
        assert(self.statementID == nil, "Statement not closed: \(self.sql)")
        if self.statementID != nil {
            self.logger.error("Statement not closed: \(self.sql)")
        }
    }
}
