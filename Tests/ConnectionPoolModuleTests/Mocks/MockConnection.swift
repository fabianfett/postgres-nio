import DequeModule
import ConnectionPoolModule
import _ConcurrencyHelpers

// Sendability enforced through the lock
final class MockConnection: PooledConnection, @unchecked Sendable {
    typealias ID = Int

    let id: ID

    private enum State {
        case running([@Sendable ((any Error)?) -> ()])
        case closing([@Sendable ((any Error)?) -> ()])
        case closed
    }

    private let lock = NIOLock()
    private var _state = State.running([])

    init(id: Int) {
        self.id = id
    }

    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ()) {
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
            closure(nil)
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
        let callbacks = self.lock.withLock { () -> [@Sendable ((any Error)?) -> ()] in
            switch self._state {
            case .running, .closed:
                return []

            case .closing(let callbacks):
                self._state = .closed
                return callbacks
            }
        }

        for callback in callbacks {
            callback(nil)
        }
    }
}

@available(macOS 14.0, *)
final class MockConnectionFactory<Clock: _Concurrency.Clock>: ConnectionFactory where Clock.Duration == Duration {
    typealias ConnectionIDGenerator = ConnectionPoolModule.ConnectionIDGenerator
    typealias Request = ConnectionRequest<MockConnection>
    typealias KeepAliveBehavior = MockPingPongBehavior
    typealias MetricsDelegate = NoOpConnectionPoolMetrics<Int>
    typealias ConnectionID = Int
    typealias Connection = MockConnection

    let lock = _ConcurrencyHelpers.NIOLock()
    var _attempts = Deque<(Int, CheckedContinuation<(MockConnection, UInt16), any Error>)>()

    func makeConnection(
        id: Int,
        for pool: ConnectionPool<MockConnectionFactory, MockConnection, Int, ConnectionIDGenerator, ConnectionRequest<MockConnection>, Int, MockPingPongBehavior, NoOpConnectionPoolMetrics<Int>, Clock>
    ) async throws -> ConnectionAndMetadata<MockConnection> {
        try await withTaskCancellationHandler {
            let result = try await withCheckedThrowingContinuation { (checkedContinuation: CheckedContinuation<(MockConnection, UInt16), any Error>) in
                self.lock.withLock {
                    self._attempts.append((id, checkedContinuation))
                }
            }

            return .init(connection: result.0, maximalStreamsOnConnection: result.1)
        } onCancel: {
            
        }
    }

    @discardableResult
    func succeedNextAttempt(maxStreams: UInt16 = 1) -> MockConnection? {
        guard let (id, checkedContinuation) = self.lock.withLock({ self._attempts.popFirst() }) else {
            return nil
        }

        let connection = MockConnection(id: id)
        defer { checkedContinuation.resume(returning: (connection, maxStreams)) }
        return connection
    }

    func failNextAttempt() {
        fatalError("TODO: Implement")
    }
}

