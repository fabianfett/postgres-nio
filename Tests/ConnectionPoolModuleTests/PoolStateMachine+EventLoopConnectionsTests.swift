import XCTest
@testable import ConnectionPoolModule

typealias TestPoolStateMachine = PoolStateMachine<
    TestConnection,
    ConnectionIDGenerator,
    TestConnection.ID,
    TestRequest<TestConnection>,
    TestRequest<TestConnection>.ID
>

final class PoolStateMachine_EventLoopConnectionsTests: XCTestCase {
    var eventLoop: EmbeddedEventLoop!
    var idGenerator: ConnectionIDGenerator!

    override func setUp() {
        self.eventLoop = EmbeddedEventLoop()
        self.idGenerator = ConnectionIDGenerator()
        super.setUp()
    }

    override func tearDown() {
        self.eventLoop = nil
        self.idGenerator = nil
        super.tearDown()
    }

    func testRefillConnections() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 4,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        XCTAssertTrue(connections.isEmpty)
        connections.refillConnections(&requests)
        XCTAssertFalse(connections.isEmpty)

        XCTAssertEqual(requests.count, 4)
        XCTAssertNil(connections.createNewDemandConnectionIfPossible())
        XCTAssertNil(connections.createNewOverflowConnectionIfPossible())
        XCTAssertEqual(connections.stats, .init(connecting: 4))
        XCTAssertEqual(connections.soonAvailable, 4)

        var requests2 = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests2)
        XCTAssertTrue(requests2.isEmpty)

        var connected: UInt16 = 0
        for request in requests {
            let newConnection = TestConnection(request: request)
            let (_, context) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
            XCTAssertEqual(context.info, .idle(availableStreams: 1, newIdle: true))
            XCTAssertEqual(context.use, .persisted)
            connected += 1
            XCTAssertEqual(connections.stats, .init(connecting: 4 - connected, idle: connected, availableStreams: connected))
            XCTAssertEqual(connections.soonAvailable, 4 - connected)
        }

        var requests3 = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests3)
        XCTAssertTrue(requests3.isEmpty)
    }

    func testMakeConnectionLeaseItAndDropItHappyPath() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests)
        XCTAssertTrue(connections.isEmpty)
        XCTAssertTrue(requests.isEmpty)

        guard let request = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to receive a connection request")
        }
        XCTAssertEqual(request, .init(eventLoop: self.eventLoop, connectionID: 0))
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(connections.soonAvailable, 1)
        XCTAssertEqual(connections.stats, .init(connecting: 1))

        let newConnection = TestConnection(request: request)
        let (_, establishedContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        XCTAssertEqual(establishedContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailable, 0)

        guard case .leasedConnection(let leasedConnection, let leaseContext) = connections.leaseConnectionOrSoonAvailableConnectionCount() else {
            return XCTFail("Expected to lease a connection")
        }
        XCTAssertEqual(leaseContext.use, .demand)
        XCTAssert(newConnection === leasedConnection)
        XCTAssertEqual(connections.stats, .init(leased: 1, leasedStreams: 1))

        let (_, releasedContext) = connections.releaseConnection(leasedConnection.id, streams: 1)
        XCTAssertEqual(releasedContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        guard let pingPongConnection = connections.keepAliveIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssert(newConnection === pingPongConnection)
        XCTAssertEqual(connections.stats, .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, pingPongContext) = connections.keepAliveSucceeded(newConnection.id) else {
            return XCTFail("Expected to get an AvailableContext")
        }
        XCTAssertEqual(pingPongContext.info, .idle(availableStreams: 1, newIdle: false))
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        guard let (closeConnection, cancelPingPong) = connections.closeConnectionIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssertTrue(cancelPingPong)
        XCTAssert(newConnection === closeConnection)
        XCTAssertEqual(connections.stats, .init(closing: 1, availableStreams: 0))

        let closeContext = connections.failConnection(newConnection.id)
        XCTAssertEqual(closeContext?.connectionsStarting, 0)
        XCTAssertTrue(connections.isEmpty)
        XCTAssertEqual(connections.stats, .init())
    }

    func testBackoffDoneCreatesANewConnectionEvenThoughRetryIsSetToFalse() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests)
        XCTAssertEqual(connections.stats, .init(connecting: 1))
        XCTAssertEqual(connections.soonAvailable, 1)
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(requests.count, 1)

        guard let request = requests.first else { return XCTFail("Expected to receive a connection request") }
        XCTAssertEqual(request, .init(eventLoop: self.eventLoop, connectionID: 0))

        let backoffEventLoop = connections.backoffNextConnectionAttempt(request.connectionID)
        XCTAssertTrue(backoffEventLoop === self.eventLoop)
        XCTAssertEqual(connections.stats, .init(backingOff: 1))

        XCTAssertEqual(connections.backoffDone(request.connectionID, retry: false), .createConnection(request))
        XCTAssertEqual(connections.stats, .init(connecting: 1))
    }

    func testBackoffDoneCancelsIdleTimerIfAPersistedConnectionIsNotRetried() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 2,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests)
        XCTAssertEqual(connections.stats, .init(connecting: 2))
        XCTAssertEqual(connections.soonAvailable, 2)
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(requests.count, 2)

        var requestIterator = requests.makeIterator()
        guard let firstRequest = requestIterator.next(), let secondRequest = requestIterator.next() else {
            return XCTFail("Expected to get two requests")
        }

        guard let thirdRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get another request")
        }
        XCTAssertEqual(connections.stats, .init(connecting: 3))

        let newSecondConnection = TestConnection(request: secondRequest)
        let (_, establishedSecondConnectionContext) = connections.newConnectionEstablished(newSecondConnection, maxStreams: 1)
        XCTAssertEqual(establishedSecondConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedSecondConnectionContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(connecting: 2, idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailable, 2)

        let newThirdConnection = TestConnection(request: thirdRequest)
        let (_, establishedThirdConnectionContext) = connections.newConnectionEstablished(newThirdConnection, maxStreams: 1)
        XCTAssertEqual(establishedThirdConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedThirdConnectionContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(connecting: 1, idle: 2, availableStreams: 2))
        XCTAssertEqual(connections.soonAvailable, 1)

        let backoffEventLoop = connections.backoffNextConnectionAttempt(firstRequest.connectionID)
        XCTAssertTrue(backoffEventLoop === self.eventLoop)
        XCTAssertEqual(connections.stats, .init(backingOff: 1, idle: 2, availableStreams: 2))

        // connection three should be moved to connection one and for this reason become permanent
        XCTAssertEqual(connections.backoffDone(firstRequest.connectionID, retry: false), .cancelIdleTimeoutTimer(newThirdConnection.id))
        XCTAssertEqual(connections.stats, .init(idle: 2, availableStreams: 2))

        XCTAssertNil(connections.closeConnectionIfIdle(newThirdConnection.id))
    }

    func testBackoffDoneReturnsNilIfOverflowConnection() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 0,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        guard let firstRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get two requests")
        }

        guard let secondRequest = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to get another request")
        }
        XCTAssertEqual(connections.stats, .init(connecting: 2))

        let newFirstConnection = TestConnection(request: firstRequest)
        let (_, establishedFirstConnectionContext) = connections.newConnectionEstablished(newFirstConnection, maxStreams: 1)
        XCTAssertEqual(establishedFirstConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedFirstConnectionContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(connecting: 1, idle: 1, availableStreams: 1))
        XCTAssertEqual(connections.soonAvailable, 1)

        let backoffEventLoop = connections.backoffNextConnectionAttempt(secondRequest.connectionID)
        XCTAssertTrue(backoffEventLoop === self.eventLoop)
        XCTAssertEqual(connections.stats, .init(backingOff: 1, idle: 1, availableStreams: 1))

        XCTAssertEqual(connections.backoffDone(secondRequest.connectionID, retry: false), .none)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        XCTAssertNotNil(connections.closeConnectionIfIdle(newFirstConnection.id))
    }

    func testPingPong() {
        var connections = TestPoolStateMachine.EventLoopConnections(
            eventLoop: self.eventLoop,
            generator: self.idGenerator,
            minimumConcurrentConnections: 1,
            maximumConcurrentConnectionSoftLimit: 4,
            maximumConcurrentConnectionHardLimit: 4,
            keepAliveReducesAvailableStreams: true
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        XCTAssertTrue(connections.isEmpty)
        connections.refillConnections(&requests)
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(connections.stats, .init(connecting: 1))

        XCTAssertEqual(requests.count, 1)
        guard let firstRequest = requests.first else { return XCTFail("Expected to have a request here") }

        let newConnection = TestConnection(request: firstRequest)
        let (_, establishedConnectionContext) = connections.newConnectionEstablished(newConnection, maxStreams: 1)
        XCTAssertEqual(establishedConnectionContext.info, .idle(availableStreams: 1, newIdle: true))
        XCTAssertEqual(establishedConnectionContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))

        guard let pingConnection = connections.keepAliveIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection")
        }
        XCTAssert(newConnection === pingConnection)
        XCTAssertEqual(connections.stats, .init(idle: 1, runningKeepAlive: 1, availableStreams: 0))

        guard let (_, afterPingIdleContext) = connections.keepAliveSucceeded(pingConnection.id) else {
            return XCTFail("Expected to receive an AvailableContext")
        }
        XCTAssertEqual(afterPingIdleContext.info, .idle(availableStreams: 1, newIdle: false))
        XCTAssertEqual(afterPingIdleContext.use, .persisted)
        XCTAssertEqual(connections.stats, .init(idle: 1, availableStreams: 1))
    }
}

final class TestConnection: PooledConnection {
    let id: Int

    let eventLoop: EventLoop

    init(id: Int, eventLoop: EventLoop) {
        self.id = id
        self.eventLoop = eventLoop
    }

    init(request: TestPoolStateMachine.ConnectionRequest) {
        self.id = request.connectionID
        self.eventLoop = request.eventLoop
    }

    func onClose(_ closure: @escaping @Sendable () -> ()) {
        preconditionFailure()
    }

    func close() {
        preconditionFailure()
    }
}

final class TestRequest<Connection: PooledConnection>: ConnectionRequestProtocol, Equatable {
    struct ID: Hashable {
        var objectID: ObjectIdentifier

        init(_ request: TestRequest) {
            self.objectID = ObjectIdentifier(request)
        }
    }

    var id: ID { ID(self) }

    let deadline: NIODeadline

    init(deadline: NIODeadline, connectionType: Connection.Type) {
        self.deadline = deadline
    }

    static func ==(lhs: TestRequest, rhs: TestRequest) -> Bool {
        lhs.id == rhs.id && lhs.deadline == rhs.deadline
    }

    func complete(with: Result<Connection, PoolError>) {

    }
}

extension TestRequest where Connection == TestConnection {
    convenience init(deadline: NIODeadline) {
        self.init(
            deadline: deadline,
            connectionType: TestConnection.self
        )
    }
}
