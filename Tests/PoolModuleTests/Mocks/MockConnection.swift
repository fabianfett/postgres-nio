import DequeModule
import NIOCore
import NIOConcurrencyHelpers
import PoolModule

// Sendability enforced through the lock
final class MockConnection: PooledConnection, @unchecked Sendable {
    typealias ID = Int

    let id: ID
    let eventLoop: EventLoop

    private enum State {
        case running([@Sendable () -> ()])
        case closing([@Sendable () -> ()])
        case closed
    }

    private let lock = NIOLock()
    private var _state = State.running([])

    init(id: Int, eventLoop: EventLoop) {
        self.id = id
        self.eventLoop = eventLoop
    }

    func onClose(_ closure: @escaping @Sendable () -> ()) {
        let enqueued = self.lock.withLock { () -> Bool in
            switch self._state {
            case .closed:
                return false

            case .running(var callbacks):
                callbacks.append(closure)
                self._state = .running(callbacks)
                return true

            case .closing(var callbacks):
                callbacks.append(closure)
                self._state = .closing(callbacks)
                return true
            }
        }

        if !enqueued {
            closure()
        }
    }

    func close() {
        self.lock.withLock {
            switch self._state {
            case .running(let callbacks):
                self._state = .closing(callbacks)

            case .closing, .closed:
                break
            }
        }
    }

    func closeIfClosing() {
        let callbacks = self.lock.withLock { () -> [@Sendable () -> ()] in
            switch self._state {
            case .running, .closed:
                return []

            case .closing(let callbacks):
                self._state = .closed
                return callbacks
            }
        }

        for callback in callbacks {
            callback()
        }
    }
}

final class MockConnectionFactory: ConnectionFactory {
    typealias ConnectionIDGenerator = PoolModule.ConnectionIDGenerator

    typealias Request = ConnectionRequest<MockConnection>

    typealias KeepAliveBehavior = MockPingPongBehavior

    typealias MetricsDelegate = NoOpConnectionPoolMetrics<Int>

    typealias ConnectionID = Int
    typealias Connection = MockConnection

    let lock = NIOConcurrencyHelpers.NIOLock()
    var _attempts = Deque<(Int, EventLoop, EventLoopPromise<ConnectionAndMetadata<MockConnection>>)>()

    func makeConnection(
        on eventLoop: NIOCore.EventLoop,
        id: Int,
        for pool: PoolModule.ConnectionPool<MockConnectionFactory, MockConnection, Int, ConnectionIDGenerator, ConnectionRequest<MockConnection>, Int, MockPingPongBehavior, NoOpConnectionPoolMetrics<Int>>) -> NIOCore.EventLoopFuture<PoolModule.ConnectionAndMetadata<MockConnection>> {
        let promise = eventLoop.makePromise(of: ConnectionAndMetadata<MockConnection>.self)
        self.lock.withLock {
            self._attempts.append((id, eventLoop, promise))
        }
        return promise.futureResult
    }

    @discardableResult
    func succeedNextAttempt() -> MockConnection? {
        guard let (id, eventLoop, promise) = self.lock.withLock({ self._attempts.popFirst() }) else {
            return nil
        }

        let connection = MockConnection(id: id, eventLoop: eventLoop)
        defer { promise.succeed(.init(connection: connection, maximalStreamsOnConnection: 1)) }
        return connection
    }

    func failNextAttempt() {

    }
}

final class MockPingPongBehavior: ConnectionKeepAliveBehavior {
    let keepAliveFrequency: TimeAmount?

    init(keepAliveFrequency: TimeAmount?) {
        self.keepAliveFrequency = keepAliveFrequency
    }

    func runKeepAlive(for connection: MockConnection) -> EventLoopFuture<Void> {
        preconditionFailure()
    }
}
