@testable import PostgresNIO
import XCTest
import Logging
import NIOEmbedded

final class ConnectionPoolTests: XCTestCase {

    func testHappyPath() {
        let logger = Logger(label: "pool-test")
        let eventLoop = EmbeddedEventLoop()
        let factory = MockConnectionFactory()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            factory: factory,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
            metricsDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            eventLoopGroup: eventLoop,
            backgroundLogger: logger
        )

        let createdConnection = factory.succeedNextAttempt()
        XCTAssertNotNil(createdConnection)

        // the same connection is reused 1000 times

        for _ in 0..<1000 {
            let connectionFuture = pool.leaseConnection(logger: logger, preferredEventLoop: eventLoop)
            var leasedConnection: MockConnection?
            XCTAssertNil(factory.succeedNextAttempt())
            XCTAssertNoThrow(leasedConnection = try connectionFuture.wait())
            XCTAssertNotNil(leasedConnection)
            XCTAssert(createdConnection === leasedConnection)

            if let leasedConnection {
                pool.releaseConnection(leasedConnection)
            }
        }


    }

}


