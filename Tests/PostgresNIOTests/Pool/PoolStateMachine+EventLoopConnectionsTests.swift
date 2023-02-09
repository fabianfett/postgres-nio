import XCTest
import NIOEmbedded
@testable import PostgresNIO

typealias TestPoolStateMachine = PoolStateMachine<TestConnection, PostgresConnection.ID.Generator, TestConnection.ID, TestRequest, Int>

final class PoolStateMachine_EventLoopConnectionsTests: XCTestCase {
    var eventLoop: EmbeddedEventLoop!
    var idGenerator: PostgresConnection.ID.Generator!

    override func setUp() {
        self.eventLoop = EmbeddedEventLoop()
        self.idGenerator = PostgresConnection.ID.Generator()
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
            maximumConcurrentConnectionHardLimit: 4
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
            let (_, context) = connections.newConnectionEstablished(newConnection)
            XCTAssertEqual(context.hasBecomeIdle, true)
            XCTAssertEqual(context.use, .persisted)
            connected += 1
            XCTAssertEqual(connections.stats, .init(connecting: 4 - connected, idle: connected))
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
            maximumConcurrentConnectionHardLimit: 4
        )

        var requests = [TestPoolStateMachine.ConnectionRequest]()
        connections.refillConnections(&requests)
        XCTAssertTrue(connections.isEmpty)
        XCTAssertTrue(requests.isEmpty)

        guard var request = connections.createNewDemandConnectionIfPossible() else {
            return XCTFail("Expected to receive a connection request")
        }
        XCTAssertEqual(request, .init(eventLoop: self.eventLoop, connectionID: 0))
        XCTAssertFalse(connections.isEmpty)
        XCTAssertEqual(connections.soonAvailable, 1)
        XCTAssertEqual(connections.stats, .init(connecting: 1))

        let newConnection = TestConnection(request: request)
        let (_, establishedContext) = connections.newConnectionEstablished(newConnection)
        XCTAssertEqual(establishedContext.hasBecomeIdle, true)
        XCTAssertEqual(establishedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1))
        XCTAssertEqual(connections.soonAvailable, 0)

        guard case .leasedConnection(let leasedConnection) = connections.leaseConnectionOrSoonAvailableConnectionCount() else {
            return XCTFail("Expected to lease a connection")
        }
        XCTAssert(newConnection === leasedConnection)
        XCTAssertEqual(connections.stats, .init(leased: 1))

        let (_, releasedContext) = connections.releaseConnection(leasedConnection.id)
        XCTAssertEqual(releasedContext.hasBecomeIdle, true)
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1))

        guard let pingPongConnection = connections.pingPongIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssert(newConnection === pingPongConnection)
        XCTAssertEqual(connections.stats, .init(pingpong: 1))

        let (_, pingPongContext) = connections.pingPongDone(newConnection.id)
        XCTAssertEqual(pingPongContext.hasBecomeIdle, false)
        XCTAssertEqual(releasedContext.use, .demand)
        XCTAssertEqual(connections.stats, .init(idle: 1))

        guard let (closeConnection, cancelPingPong) = connections.closeConnectionIfIdle(newConnection.id) else {
            return XCTFail("Expected to get a connection for ping pong")
        }
        XCTAssertTrue(cancelPingPong)
        XCTAssert(newConnection === closeConnection)
        XCTAssertEqual(connections.stats, .init(closing: 1))

        let closeContext = connections.failConnection(newConnection.id)
        XCTAssertEqual(closeContext?.connectionsStarting, 0)
        XCTAssertTrue(connections.isEmpty)
        XCTAssertEqual(connections.stats, .init())
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
}

final class TestRequest: ConnectionRequest {
    let id: Int

    let preferredEventLoop: EventLoop?

    let deadline: NIODeadline

    init(id: Int, deadline: NIODeadline, preferredEventLoop: EventLoop?) {
        self.id = id
        self.deadline = deadline
        self.preferredEventLoop = preferredEventLoop
    }
}
