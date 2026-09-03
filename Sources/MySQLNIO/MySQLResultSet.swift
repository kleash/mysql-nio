/// One result set exactly as the server described it: its column
/// definitions and the rows that followed them.
///
/// Keeping the columns *separate* from the rows is the whole point. A
/// caller handed only `[MySQLRow]` cannot represent — or even detect — a
/// result set that matched nothing: column definitions travel on the
/// individual rows, so zero rows means zero column information, and an
/// empty `SELECT` becomes indistinguishable from a statement that returns
/// no result set at all (`INSERT`, `USE`, DDL, …). A client that wants to
/// draw an empty grid with the right headers needs to tell those two
/// apart.
public struct MySQLResultSet: Sendable {
    /// The columns the server announced for this result set, in order.
    /// Non-empty for anything result-set-shaped, including a `SELECT` that
    /// matched no rows.
    public let columns: [MySQLProtocol.ColumnDefinition41]

    /// The rows that followed. Empty when nothing matched.
    public let rows: [MySQLRow]

    public init(columns: [MySQLProtocol.ColumnDefinition41], rows: [MySQLRow]) {
        self.columns = columns
        self.rows = rows
    }
}
