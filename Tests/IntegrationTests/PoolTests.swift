import PostgresNIO
import XCTest
import NIOPosix
import Logging
@preconcurrency import Atomics

final class PoolTests: XCTestCase {

    func testGetConnection() {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        var configuration = PostgresConnectionPoolConfiguration()
        configuration.minimumConnectionCount = 8
        configuration.maximumConnectionSoftLimit = 64
        configuration.maximumConnectionHardLimit = 64

        let connectionConfig = PostgresConnection.Configuration(
            connection: .init(host: "localhost"),
            authentication: .init(username: "test_username", database: "test_database", password: "test_password"),
            tls: .disable
        )

        let pool = PostgresConnectionPool(
            configuration: configuration,
            factory: Factory(configuration: connectionConfig),
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        let onCounter = ManagedAtomic(0)
        let offCounter = ManagedAtomic(0)

        var futures = [EventLoopFuture<PostgresQueryResult>]()
        futures.reserveCapacity(1000)

        XCTAssertNoThrow(try eventLoopGroup.next().scheduleTask(in: .seconds(1), {}).futureResult.wait())

        for _ in 0..<10000 {
            let eventLoop = eventLoopGroup.next()
            let future = pool.withConnection(logger: logger, preferredEventLoop: eventLoop) {
                connection -> EventLoopFuture<PostgresQueryResult> in

                if eventLoop === connection.eventLoop {
                    onCounter.wrappingIncrement(ordering: .relaxed)
                } else {
                    offCounter.wrappingIncrement(ordering: .relaxed)
                }

                return connection.query("SELECT 1", logger: logger)
            }

            futures.append(future)
        }

        let future = EventLoopFuture.andAllSucceed(futures, on: eventLoopGroup.next())
        XCTAssertNoThrow(try future.wait())

        logger.info("Result", metadata: [
            "on-el": "\(onCounter.load(ordering: .relaxed))",
            "off-el": "\(offCounter.load(ordering: .relaxed))",
        ])
    }



}

struct Factory: PostgresConnectionFactory {
    let configuration: PostgresConnection.Configuration

    init(configuration: PostgresConnection.Configuration) {
        self.configuration = configuration
    }

    func makeConnection(
        on eventLoop: EventLoop,
        id: PostgresConnection.ID,
        backgroundLogger: Logger
    ) -> EventLoopFuture<PostgresConnection> {
        PostgresConnection.connect(
            on: eventLoop,
            configuration: self.configuration,
            id: id,
            logger: backgroundLogger
        )
    }
}
