import PostgresNIO
import XCTest
import NIOPosix
import Logging
@preconcurrency import Atomics

final class PostgresClientTests: XCTestCase {

    func testGetConnection() {
        var mlogger = Logger(label: "test")
        mlogger.logLevel = .debug
        let logger = mlogger
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 8)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        var clientConfig = PostgresClient.Configuration()
        clientConfig.pool.minimumConnectionCount = 0
        clientConfig.pool.maximumConnectionSoftLimit = 8*4
        clientConfig.pool.maximumConnectionHardLimit = 12*4
        clientConfig.pool.keepAliveFrequency = .seconds(5)
        clientConfig.pool.connectionIdleTimeout = .seconds(15)

        clientConfig.server.host = "postgres-nio-tests.vsl"
        clientConfig.authentication.username = "postgres"
        clientConfig.authentication.database = "postgres"
        clientConfig.authentication.password = "password"

        var maybeClient: PostgresClient?
        XCTAssertNoThrow(maybeClient = try PostgresClient(configuration: clientConfig, eventLoopGroup: eventLoopGroup, backgroundLogger: logger))
        guard let client = maybeClient else { return XCTFail("Expected to have a client here") }
//        defer { XCTAssertNoThrow(try client.syncShutdown()) }

        let onCounter = ManagedAtomic(0)
        let offCounter = ManagedAtomic(0)

        var futures = [EventLoopFuture<PostgresQueryResult>]()
        futures.reserveCapacity(1000)

        XCTAssertNoThrow(try eventLoopGroup.next().scheduleTask(in: .seconds(1), {}).futureResult.wait())

        for _ in 0..<10000 {
            let eventLoop = eventLoopGroup.next()
            let future = client.withConnection(logger: logger, preferredEventLoop: eventLoop) {
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

struct MockConnectionFactory: ConnectionFactory {
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