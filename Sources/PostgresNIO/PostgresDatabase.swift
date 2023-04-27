import NIOCore
import Logging

@available(*, deprecated, message: "PostgresNIO will not provide a Postgres abstraction going forward.")
public protocol PostgresDatabase {
    var logger: Logger { get }
    var eventLoop: EventLoop { get }
    func send(
        _ request: PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void>
    
    func withConnection<T>(_ closure: @escaping (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T>
}

@available(*, deprecated, message: "PostgresNIO will not provide a Postgres abstraction going forward.")
extension PostgresDatabase {
    public func logging(to logger: Logger) -> PostgresDatabase {
        _PostgresDatabaseCustomLogger(database: self, logger: logger)
    }
}

@available(*, deprecated, message: "PostgresNIO will not provide a Postgres abstraction going forward.")
private struct _PostgresDatabaseCustomLogger {
    let database: PostgresDatabase
    let logger: Logger
}

@available(*, deprecated, message: "Conformance is deprecated, since protocol is deprecated.")
extension _PostgresDatabaseCustomLogger: PostgresDatabase {
    var eventLoop: EventLoop {
        self.database.eventLoop
    }
    
    func send(_ request: PostgresRequest, logger: Logger) -> EventLoopFuture<Void> {
        self.database.send(request, logger: logger)
    }
    
    func withConnection<T>(_ closure: @escaping (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        self.database.withConnection(closure)
    }
}
