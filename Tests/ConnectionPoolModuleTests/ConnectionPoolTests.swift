@testable import ConnectionPoolModule
import XCTest
import NIOEmbedded

@available(macOS 14.0, *)
final class ConnectionPoolTests: XCTestCase {

    func testHappyPath() async {
        let eventLoop = EmbeddedEventLoop()
        let factory = MockConnectionFactory<ContinuousClock>()

        var config = ConnectionPoolConfiguration(coreCount: 1)
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            factory: factory,
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil),
            metricsDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        )

        // the same connection is reused 1000 times

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            try? await Task.sleep(for: .milliseconds(50))

            let createdConnection = factory.succeedNextAttempt()
            XCTAssertNotNil(createdConnection)

            do {
                for _ in 0..<1000 {
                    async let connectionFuture = try await pool.leaseConnection()
                    var leasedConnection: MockConnection?
                    XCTAssertNil(factory.succeedNextAttempt())
                    leasedConnection = try await connectionFuture
                    XCTAssertNotNil(leasedConnection)
                    XCTAssert(createdConnection === leasedConnection)

                    if let leasedConnection {
                        pool.releaseConnection(leasedConnection)
                    }
                }
            } catch {
                XCTFail("Unexpected error: \(error)")
            }

            taskGroup.cancelAll()
        }
    }
}


