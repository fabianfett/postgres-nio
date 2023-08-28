import NIOCore
import NIOEmbedded
import XCTest
@testable import ConnectionPoolModule

typealias TestPoolStateMachine = PoolStateMachine<
    MockConnection,
    ConnectionIDGenerator,
    MockConnection.ID,
    MockRequest,
    MockRequest.ID,
    MockTimerCancellationToken
>

final class PoolStateMachineTests: XCTestCase {
    var configuration = PoolConfiguration()
    let eventLoop = EmbeddedEventLoop()

    override func setUp() {
        self.configuration.minimumConnectionCount = 2
        self.configuration.maximumConnectionSoftLimit = 4
        self.configuration.maximumConnectionHardLimit = 6
        self.configuration.keepAliveDuration = .seconds(10)
    }

    func testHappyPath() {
        var (stateMachine, connections) = TestPoolStateMachine.createAndRefillConnections(
            self.configuration,
            eventLoopGroup: self.eventLoop
        )

        let leaseRequest = MockRequest()
        let leaseAction = stateMachine.leaseConnection(leaseRequest)
        guard case .leaseConnection(let requests, let leasedConnection) = leaseAction.request else {
            return XCTFail("Unexpected request action")
        }
        XCTAssertEqual(requests.count, 1)
        XCTAssertEqual(requests.first, leaseRequest)
        XCTAssert(connections.contains(where: { $0.id == leasedConnection.id }))

//        XCTAssertEqual(leaseAction.connection, .cancelKeepAliveTimer(leasedConnection.id))
//
//        let releaseAction = stateMachine.releaseConnection(leasedConnection, streams: 1)
//        XCTAssertEqual(releaseAction.request, .none)
//        XCTAssertEqual(releaseAction.connection, .scheduleKeepAliveTimer(leasedConnection.id, on: leasedConnection.eventLoop))
    }

    func testMoreConnectionsAreCreated() {
//        var (stateMachine, connections) = TestPoolStateMachine.createAndRefillConnections(
//            self.configuration,
//            eventLoopGroup: self.eventLoop
//        )
//
//        for _ in 0..<2 {
//            let leaseRequest = TestRequest(deadline: .now() + .seconds(2))
//            let leaseAction = stateMachine.leaseConnection(leaseRequest)
//            guard case .leaseConnection(let requests, let leasedConnection) = leaseAction.request else {
//                return XCTFail("Unexpected request action")
//            }
//            XCTAssertEqual(requests.count, 1)
//            XCTAssertEqual(requests.first, leaseRequest)
//            XCTAssert(connections.contains(where: { $0.id == leasedConnection.id }))
//            XCTAssertEqual(leaseAction.connection, .cancelKeepAliveTimer(leasedConnection.id))
//        }
//
//        let connRequests = (0..<4).compactMap { (_) -> (TestPoolStateMachine.ConnectionRequest, TestRequest<TestConnection>)? in
//            let leaseRequest = TestRequest(deadline: .now() + .seconds(2))
//            let leaseAction = stateMachine.leaseConnection(leaseRequest)
//            XCTAssertEqual(leaseAction.request, .none)
//            guard case .createConnection(let connRequest) = leaseAction.connection else {
//                XCTFail("Expected to get a connection creation action")
//                return nil
//            }
//            return (connRequest, leaseRequest)
//        }
//
//        //
//        let leaseRequest4 = TestRequest(deadline: .now() + .seconds(2))
//        let leaseAction4 = stateMachine.leaseConnection(leaseRequest4)
//        XCTAssertEqual(leaseAction4.request, .none)
//        XCTAssertEqual(leaseAction4.connection, .none)
//
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
    ) -> (TestPoolStateMachine, [MockConnection]) {
        var stateMachine = TestPoolStateMachine(
            configuration: configuration,
            generator: .init(),
            timerCancellationTokenType: MockTimerCancellationToken.self
        )

        let initialConnectionRequests = stateMachine.refillConnections()
        XCTAssertEqual(initialConnectionRequests.count, configuration.minimumConnectionCount, file: file, line: line)

        let expectedTimers = configuration.keepAliveDuration != nil ? 1 : 0

        let connections = initialConnectionRequests.compactMap { request -> MockConnection? in
            let newConnection = MockConnection(id: request.connectionID)
            let connectionEstablishedAction = stateMachine.connectionEstablished(newConnection, maxStreams: 1)
            XCTAssertEqual(connectionEstablishedAction.request, .none, file: file, line: line)
            guard case .scheduleTimers(let timers) = connectionEstablishedAction.connection else {
                XCTFail("Expected schedule ping timer connection action")
                return nil
            }
            XCTAssertEqual(timers.count, expectedTimers, file: file, line: line)
            for timer in timers {
                XCTAssertNil(stateMachine.timerScheduled(timer, cancelContinuation: .init(timer)))
            }
            return newConnection
        }

        XCTAssertEqual(connections.count, configuration.minimumConnectionCount)

        return (stateMachine, connections)
    }
}
