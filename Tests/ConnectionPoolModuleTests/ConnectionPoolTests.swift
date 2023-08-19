@testable import ConnectionPoolModule
import XCTest
import NIOEmbedded

@available(macOS 14.0, *)
final class ConnectionPoolTests: XCTestCase {

    func testHappyPath() {
        let eventLoop = EmbeddedEventLoop()
        let factory = MockConnectionFactory()

        var config = ConnectionPoolConfiguration(coreCount: 1)
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            factory: factory,
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
            metricsDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            eventLoopGroup: eventLoop
        )

        let createdConnection = factory.succeedNextAttempt()
        XCTAssertNotNil(createdConnection)

        // the same connection is reused 1000 times

        for _ in 0..<1000 {
            let connectionFuture = pool.leaseConnection()
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


