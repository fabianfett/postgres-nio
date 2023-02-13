@testable import PostgresNIO
import XCTest
import NIOEmbedded

final class PoolStateMachineTests: XCTestCase {
    var configuration = PoolConfiguration()
    let eventLoop = EmbeddedEventLoop()

    override func setUp() {
        self.configuration.minimumConnectionCount = 2
        self.configuration.maximumConnectionSoftLimit = 4
        self.configuration.maximumConnectionHardLimit = 6
    }

    func testHappyPath() {
        var (stateMachine, connections) = TestPoolStateMachine.createAndRefillConnections(
            self.configuration,
            eventLoopGroup: self.eventLoop
        )

        let leaseRequest = TestRequest(deadline: .now() + .seconds(2), preferredEventLoop: nil)
        let leaseAction = stateMachine.leaseConnection(leaseRequest)
        guard case .leaseConnection(leaseRequest, let leasedConnection, cancelTimeout: false) = leaseAction.request else {
            return XCTFail("Unexpected request action")
        }
        XCTAssert(connections.contains(where: { $0.id == leasedConnection.id }))

        XCTAssertEqual(leaseAction.connection, .cancelPingTimer(leasedConnection.id))

        let releaseAction = stateMachine.releaseConnection(leasedConnection)
        XCTAssertEqual(releaseAction.request, .none)
        XCTAssertEqual(releaseAction.connection, .schedulePingTimer(leasedConnection.id, on: leasedConnection.eventLoop))
    }

    func testMoreConnectionsAreCreated() {
        var (stateMachine, connections) = TestPoolStateMachine.createAndRefillConnections(
            self.configuration,
            eventLoopGroup: self.eventLoop
        )

        for _ in 0..<2 {
            let leaseRequest = TestRequest(deadline: .now() + .seconds(2), preferredEventLoop: nil)
            let leaseAction = stateMachine.leaseConnection(leaseRequest)
            guard case .leaseConnection(leaseRequest, let leasedConnection, cancelTimeout: false) = leaseAction.request else {
                return XCTFail("Unexpected request action")
            }
            XCTAssert(connections.contains(where: { $0.id == leasedConnection.id }))
            XCTAssertEqual(leaseAction.connection, .cancelPingTimer(leasedConnection.id))
        }

        let connRequests = (0..<4).compactMap { (_) -> (TestPoolStateMachine.ConnectionRequest, TestRequest)? in
            let leaseRequest = TestRequest(deadline: .now() + .seconds(2), preferredEventLoop: nil)
            let leaseAction = stateMachine.leaseConnection(leaseRequest)
            XCTAssertEqual(leaseAction.request, .scheduleRequestTimeout(for: leaseRequest, on: self.eventLoop))
            guard case .createConnection(let connRequest) = leaseAction.connection else {
                XCTFail("Expected to get a connection creation action")
                return nil
            }
            return (connRequest, leaseRequest)
        }

        //
        let leaseRequest4 = TestRequest(deadline: .now() + .seconds(2), preferredEventLoop: nil)
        let leaseAction4 = stateMachine.leaseConnection(leaseRequest4)
        XCTAssertEqual(leaseAction4.request, .scheduleRequestTimeout(for: leaseRequest4, on: self.eventLoop))
        XCTAssertEqual(leaseAction4.connection, .none)

//        guard case .leaseConnection(leaseRequest3, let leasedConnection, cancelTimeout: false) = leaseAction3.request else {
//            return XCTFail("Unexpected request action")
//        }
//        XCTAssert(connections.contains(where: { $0.id == leasedConnection.id }))
//        XCTAssertEqual(leaseAction3.connection, .cancelPingTimer(leasedConnection.id))
//
//
//        let releaseAction = stateMachine.releaseConnection(leasedConnection)
//        XCTAssertEqual(releaseAction.request, .none)
//        XCTAssertEqual(releaseAction.connection, .schedulePingTimer(leasedConnection.id, on: leasedConnection.eventLoop))
    }
}

extension TestPoolStateMachine {
    static func createAndRefillConnections(
        _ configuration: PoolConfiguration,
        eventLoopGroup: EventLoopGroup,
        file: StaticString = #filePath,
        line: UInt = #line
    ) -> (TestPoolStateMachine, [TestConnection]) {
        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            eventLoopGroup: eventLoopGroup
        )

        let initialConnectionRequests = stateMachine.refillConnections()
        XCTAssertEqual(initialConnectionRequests.count, 2, file: file, line: line)

        let connections = initialConnectionRequests.compactMap { request -> TestConnection? in
            let newConnection = TestConnection(request: request)
            let connectionEstablishedAction = stateMachine.connectionEstablished(newConnection)
            XCTAssertEqual(connectionEstablishedAction.request, .none, file: file, line: line)
            guard case .schedulePingTimer(newConnection.id, on: _) = connectionEstablishedAction.connection else {
                XCTFail("Expected schedule ping timer connection action")
                return nil
            }
            return newConnection
        }

        XCTAssertEqual(connections.count, configuration.minimumConnectionCount)

        return (stateMachine, connections)
    }
}