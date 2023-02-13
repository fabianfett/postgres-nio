import PostgresNIO
import NIOCore
import NIOConcurrencyHelpers

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
    typealias ConnectionID = Int
    typealias Connection = MockConnection

    func makeConnection(on eventLoop: EventLoop, id: Int, backgroundLogger: Logger) -> EventLoopFuture<MockConnection> {
        preconditionFailure()
    }

    func succeedNextAttempt() {

    }

    func failNextAttempt() {

    }
}
