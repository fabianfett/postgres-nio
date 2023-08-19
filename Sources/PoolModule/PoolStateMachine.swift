#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

@usableFromInline
struct PoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes idle connections,
    /// the `ConnectionPool` will initiate new outbound connections proactively
    /// to avoid the number of available connections dropping below this number.
    @usableFromInline
    var minimumConnectionCount: Int = 0

    /// The maximum number of connections to for this pool, to be preserved.
    @usableFromInline
    var maximumConnectionSoftLimit: Int = 10

    @usableFromInline
    var maximumConnectionHardLimit: Int = 10

    @usableFromInline
    var keepAlive: Bool = false
}

@usableFromInline
@available(macOS 14.0, *)
struct PoolStateMachine<
    Connection: PooledConnection,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    ConnectionID,
    Request: ConnectionRequestProtocol,
    RequestID
> where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID {

    @usableFromInline
    struct Action: Equatable {
        @usableFromInline let request: RequestAction
        @usableFromInline let connection: ConnectionAction

        @inlinable
        init(request: RequestAction, connection: ConnectionAction) {
            self.request = request
            self.connection = connection
        }

        @inlinable
        static func none() -> Action { Action(request: .none, connection: .none) }
    }

    @usableFromInline
    enum ConnectionAction: Equatable {
        @usableFromInline
        struct Shutdown: Equatable {
            @usableFromInline
            struct ConnectionToClose: Equatable {
                @usableFromInline var cancelIdleTimer: Bool
                @usableFromInline var cancelKeepAliveTimer: Bool
                @usableFromInline var connection: Connection

                @inlinable
                init(cancelIdleTimer: Bool, cancelKeepAliveTimer: Bool, connection: Connection) {
                    self.cancelIdleTimer = cancelIdleTimer
                    self.cancelKeepAliveTimer = cancelKeepAliveTimer
                    self.connection = connection
                }

                @inlinable
                static func ==(lhs: Self, rhs: Self) -> Bool {
                    lhs.cancelIdleTimer == rhs.cancelIdleTimer && lhs.cancelKeepAliveTimer == rhs.cancelKeepAliveTimer && lhs.connection === rhs.connection
                }
            }

            @usableFromInline var connections: [ConnectionToClose]
            @usableFromInline var backoffTimersToCancel: [ConnectionID]

            init() {
                self.connections = []
                self.backoffTimersToCancel = []
            }
        }

        case scheduleTimer(Timer)

        case makeConnection(ConnectionRequest)
        case runKeepAlive(Connection)
        case closeConnection(Connection)
        case shutdown(Shutdown)

        case none

        @usableFromInline
        static func ==(lhs: Self, rhs: Self) -> Bool {
            switch (lhs, rhs) {
            case (.scheduleTimer(let lhs), .scheduleTimer(let rhs)):
                return lhs == rhs
            case (.makeConnection(let lhs), .makeConnection(let rhs)):
                return lhs == rhs
            case (.runKeepAlive(let lhs), .runKeepAlive(let rhs)):
                return lhs === rhs
            case (.closeConnection(let lhsConn), .closeConnection(let rhsConn)):
                return lhsConn === rhsConn
            case (.shutdown(let lhs), .shutdown(let rhs)):
                return lhs == rhs
            case (.none, .none):
                return true
            default:
                return false
            }
        }
    }

    @usableFromInline
    enum RequestAction: Equatable {
        case leaseConnection(RequestCollection<Request>, Connection)

        case failRequest(Request, PoolError)
        case failRequests(RequestCollection<Request>, PoolError)

        case none

        @usableFromInline
        static func ==(lhs: Self, rhs: Self) -> Bool {
            switch (lhs, rhs) {
            case (.leaseConnection(let lhsRequests, let lhsConn), .leaseConnection(let rhsRequests, let rhsConn)):
                guard lhsRequests.count == rhsRequests.count else { return false }
                var lhsIterator = lhsRequests.makeIterator()
                var rhsIterator = rhsRequests.makeIterator()
                while let lhsNext = lhsIterator.next(), let rhsNext = rhsIterator.next() {
                    guard lhsNext.id == rhsNext.id else { return false }
                }
                return lhsConn === rhsConn
            case (.failRequest(let lhsRequest, let lhsError), .failRequest(let rhsRequest, let rhsError)):
                return lhsRequest.id == rhsRequest.id && lhsError == rhsError
            case (.failRequests(let lhsRequests, let lhsError), .failRequests(let rhsRequests, let rhsError)):
                return Set(lhsRequests.lazy.map(\.id)) == Set(rhsRequests.lazy.map(\.id)) && lhsError == rhsError
            case (.none, .none):
                return true
            default:
                return false
            }
        }
    }

    @usableFromInline
    enum PoolState {
        case running
        case shuttingDown(graceful: Bool)
        case shutDown
    }

    @usableFromInline
    struct ConnectionRequest: Equatable {
        @usableFromInline var connectionID: ConnectionID

        @usableFromInline
        init(connectionID: ConnectionID) {
            self.connectionID = connectionID
        }
    }

    @usableFromInline
    struct Timer: Equatable {
        @usableFromInline
        enum Usecase {
            case backoff
            case idleTimeout
            case keepAlive
        }

        @usableFromInline
        var connectionID: ConnectionID

        @usableFromInline
        var timerID: Int

        @usableFromInline
        var duration: Duration

        @usableFromInline
        var usecase: Usecase
    }

    @usableFromInline let configuration: PoolConfiguration
    @usableFromInline let generator: ConnectionIDGenerator

    @usableFromInline
    private(set) var connections: EventLoopConnections
    @usableFromInline
    private(set) var requestQueue: RequestQueue
    @usableFromInline
    private(set) var poolState: PoolState = .running
    @usableFromInline
    private(set) var cacheNoMoreConnectionsAllowed: Bool = false

    @usableFromInline
    private(set) var failedConsecutiveConnectionAttempts: Int = 0
    
    @inlinable
    init(
        configuration: PoolConfiguration,
        generator: ConnectionIDGenerator
    ) {
        self.configuration = configuration
        self.generator = generator
        self.connections = EventLoopConnections(
            generator: generator,
            minimumConcurrentConnections: configuration.minimumConnectionCount,
            maximumConcurrentConnectionSoftLimit: configuration.maximumConnectionSoftLimit,
            maximumConcurrentConnectionHardLimit: configuration.maximumConnectionHardLimit,
            keepAliveReducesAvailableStreams: true
        )
        self.requestQueue = RequestQueue()
    }

    mutating func refillConnections() -> [ConnectionRequest] {
        var request = [ConnectionRequest]()
        request.reserveCapacity(self.configuration.minimumConnectionCount)

        self.connections.refillConnections(&request)

        return request
    }

    @inlinable
    mutating func leaseConnection(_ request: Request) -> Action {
        func connectionActionForLease(_ connectionID: ConnectionID, info: ConnectionLeasedInfo) -> ConnectionAction {
            fatalError()
//            switch (self.configuration.keepAlive, info.use, info.wasIdle) {
//            case (_, .demand, false), (_, .persisted, false):
//                return .none
//            case (true, .demand, true):
//                return .cancelKeepAliveAndIdleTimeoutTimer(connectionID)
//            case (false, .demand, true):
//                return .cancelIdleTimeoutTimer(connectionID)
//            case (true, .persisted, true):
//                return .cancelKeepAliveTimer(connectionID)
//            case (false, .persisted, true):
//                return .none
//            case (_, .overflow, _):
//                preconditionFailure("Overflow connections should never be available on fast turn around")
//            }
        }

        switch self.poolState {
        case .running:
            break

        case .shuttingDown, .shutDown:
            return .init(
                request: .failRequest(request, PoolError.poolShutdown),
                connection: .none
            )
        }

        if !self.requestQueue.isEmpty && self.cacheNoMoreConnectionsAllowed {
            self.requestQueue.queue(request)
            return .none()
        }

        var soonAvailable: UInt16 = 0

        // check if any other EL has an idle connection
        switch self.connections.leaseConnectionOrSoonAvailableConnectionCount() {
        case .leasedConnection(let connection, let info):
            return .init(
                request: .leaseConnection(.init(request), connection),
                connection: connectionActionForLease(connection.id, info: info)
            )

        case .startingCount(let count):
            soonAvailable += count
        }

        // we tried everything. there is no connection available. now we must check, if and where we
        // can create further connections. but first we must enqueue the new request

        self.requestQueue.queue(request)

        let requestAction = RequestAction.none

        if soonAvailable >= self.requestQueue.count {
            // if more connections will be soon available then we have waiters, we don't need to
            // create further new connections.
            return .init(
                request: requestAction,
                connection: .none
            )
        }

        // check if any other EL has an idle connection
        if let request = self.connections.createNewDemandConnectionIfPossible() {
            return .init(
                request: requestAction,
                connection: .makeConnection(request)
            )
        }

        self.cacheNoMoreConnectionsAllowed = true

        // no new connections allowed:
        return .init(request: requestAction, connection: .none)
    }

    @inlinable
    mutating func releaseConnection(_ connection: Connection, streams: UInt16) -> Action {
        let (index, context) = self.connections.releaseConnection(connection.id, streams: streams)
        return self.handleAvailableConnection(index: index, availableContext: context)
    }

    mutating func cancelRequest(id: RequestID) -> Action {
        guard let request = self.requestQueue.remove(id) else {
            return .none()
        }

        return .init(
            request: .failRequest(request, PoolError.requestCancelled),
            connection: .none
        )
    }

    @inlinable
    mutating func connectionEstablished(_ connection: Connection, maxStreams: UInt16) -> Action {
        let (index, context) = self.connections.newConnectionEstablished(connection, maxStreams: maxStreams)
        return self.handleAvailableConnection(index: index, availableContext: context)
    }

    @inlinable
    mutating func timerScheduled(_ timer: Timer, cancelContinuation: CheckedContinuation<Void, Never>) -> CheckedContinuation<Void, Never>? {
        fatalError()
    }

    @inlinable
    mutating func timerTriggered(_ timer: Timer) -> Action {
        fatalError()
//        switch timer.usecase {
//        case .backoff:
//            return self.connectionCreationBackoffDone(timer.connectionID)
//        case .keepAlive:
//            return self.connectionKeepAliveTimerTriggered(timer.connectionID)
//        case .idle:
//            return self.connectionIdleTimerTriggered(timer.connectionID)
//        }
    }

    @inlinable
    mutating func connectionEstablishFailed(_ error: Error, for request: ConnectionRequest) -> Action {
        fatalError()
//        self.failedConsecutiveConnectionAttempts += 1
//
//        self.connections.backoffNextConnectionAttempt(request.connectionID)
//        let backoff = Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
//        return .init(request: .none, connection: .scheduleBackoffTimer(request.connectionID, backoff: backoff))
    }

    @inlinable
    mutating func connectionCreationBackoffDone(_ connectionID: ConnectionID) -> Action {
        let soonAvailable = self.connections.soonAvailable
        let retry = (soonAvailable - 1) < self.requestQueue.count

        switch self.connections.backoffDone(connectionID, retry: retry) {
        case .createConnection(let request):
            return .init(request: .none, connection: .makeConnection(request))
        case .cancelIdleTimeoutTimer(let connectionID):
            fatalError()
//            return .init(request: .none, connection: .cancelIdleTimeoutTimer(connectionID))
        case .none:
            return .none()
        }
    }

    @inlinable
    mutating func connectionKeepAliveTimerTriggered(_ connectionID: ConnectionID) -> Action {
        precondition(self.configuration.keepAlive)
        precondition(self.requestQueue.isEmpty)

        guard let connection = self.connections.keepAliveIfIdle(connectionID) else {
            return .none()
        }
        return .init(request: .none, connection: .runKeepAlive(connection))
    }

    @inlinable
    mutating func connectionKeepAliveDone(_ connection: Connection) -> Action {
        precondition(self.configuration.keepAlive)
        guard let (index, context) = self.connections.keepAliveSucceeded(connection.id) else {
            return .none()
        }
        return self.handleAvailableConnection(index: index, availableContext: context)
    }

    @inlinable
    mutating func connectionIdleTimerTriggered(_ connectionID: ConnectionID) -> Action {
        precondition(self.requestQueue.isEmpty)

        guard let (connection, cancelKeepAliveTimer) = self.connections.closeConnectionIfIdle(connectionID) else {
            return .none()
        }

        self.cacheNoMoreConnectionsAllowed = false
        fatalError()
//        return .init(request: .none, connection: .closeConnection(connection, cancelKeepAliveTimer: cancelKeepAliveTimer))
    }

    @inlinable
    mutating func connectionClosed(_ connection: Connection) -> Action {
        self.cacheNoMoreConnectionsAllowed = false
        return .none()
    }

    struct CleanupAction {
        struct ConnectionToDrop {
            var connection: Connection
            var keepAliveTimer: Bool
            var idleTimer: Bool
        }

        var connections: [ConnectionToDrop]
        var requests: [Request]
    }

    mutating func triggerGracefulShutdown() -> Action {
        fatalError()
    }

    mutating func triggerForceShutdown() -> Action {
        switch self.poolState {
        case .running:
            self.poolState = .shuttingDown(graceful: false)
            var shutdown = ConnectionAction.Shutdown()
            self.connections.shutdown(&shutdown)
            
            return .init(
                request: .failRequests(self.requestQueue.removeAll(), PoolError.poolShutdown),
                connection: .shutdown(shutdown)
            )

        case .shuttingDown:
            return .none()

        case .shutDown:
            return .init(request: .none, connection: .none)
        }
    }

    @inlinable
    /*private*/ mutating func handleAvailableConnection(
        index: Int,
        availableContext: EventLoopConnections.AvailableConnectionContext
    ) -> Action {
        // this connection was busy before
        var requests = RequestCollection<Request>()
        requests.reserveCapacity(Int(availableContext.info.availableStreams))
        for _ in 0..<availableContext.info.availableStreams {
            if let request = self.requestQueue.pop() {
                requests.append(request)
            } else {
                break
            }
        }
        if !requests.isEmpty {
            let (connection, _) = self.connections.leaseConnection(at: index, streams: UInt16(requests.count))
            return .init(
                request: .leaseConnection(requests, connection),
                connection: .none
            )
        }

        func makeIdleConnectionAction(for connectionID: ConnectionID, scheduleTimeout: Bool) -> ConnectionAction {
            fatalError()
//            switch (scheduleTimeout, self.configuration.keepAlive) {
//            case (false, false):
//                return .none
//            case (true, false):
//                return .scheduleIdleTimeoutTimer(connectionID)
//            case (false, true):
//                return .scheduleKeepAliveTimer(connectionID)
//            case (true, true):
//                return .scheduleKeepAliveAndIdleTimeoutTimer(connectionID)
//            }
        }

        switch (availableContext.use, availableContext.info) {
        case (.persisted, .idle):
            fatalError()
//            let connectionID = self.connections.parkConnection(at: index)
//            return .init(
//                request: .none,
//                connection: makeIdleConnectionAction(for: connectionID, scheduleTimeout: false)
//            )

        case (.demand, .idle):

            guard case .idle(availableStreams: _, let newIdle) = availableContext.info else {
                preconditionFailure()
            }

            let connectionID = self.connections.parkConnection(
                at: index,
                scheduleKeepAliveTimer: self.configuration.keepAlive,
                scheduleIdleTimeoutTimer: newIdle
            )
            fatalError()

//            return .init(
//                request: .none,
//                connection: makeIdleConnectionAction(for: connectionID, scheduleTimeout: newIdle)
//            )

        case (.overflow, .idle):
            fatalError()
//            let connection = self.connections.closeConnection(at: index)
//            return .init(request: .none, connection: .closeConnection(connection, cancelKeepAliveTimer: false))

        case (_, .leased):
            return .none()
        }
    }
}

@available(macOS 14.0, *)
extension PoolStateMachine {
    /// Calculates the delay for the next connection attempt after the given number of failed `attempts`.
    ///
    /// Our backoff formula is: 100ms * 1.25^(attempts - 1) that is capped of at 1 minute.
    /// This means for:
    ///   -  1 failed attempt :  100ms
    ///   -  5 failed attempts: ~300ms
    ///   - 10 failed attempts: ~930ms
    ///   - 15 failed attempts: ~2.84s
    ///   - 20 failed attempts: ~8.67s
    ///   - 25 failed attempts: ~26s
    ///   - 29 failed attempts: ~60s (max out)
    ///
    /// - Parameter attempts: number of failed attempts in a row
    /// - Returns: time to wait until trying to establishing a new connection
    @usableFromInline
    static func calculateBackoff(failedAttempt attempts: Int) -> Duration {
        // Our backoff formula is: 100ms * 1.25^(attempts - 1) that is capped of at 1minute
        // This means for:
        //   -  1 failed attempt :  100ms
        //   -  5 failed attempts: ~300ms
        //   - 10 failed attempts: ~930ms
        //   - 15 failed attempts: ~2.84s
        //   - 20 failed attempts: ~8.67s
        //   - 25 failed attempts: ~26s
        //   - 29 failed attempts: ~60s (max out)

        let start = Double(100_000_000)
        let backoffNanosecondsDouble = start * pow(1.25, Double(attempts - 1))

        // Cap to 60s _before_ we convert to Int64, to avoid trapping in the Int64 initializer.
        let backoffNanoseconds = Int64(min(backoffNanosecondsDouble, Double(60_000_000_000)))

        let backoff = Duration.nanoseconds(backoffNanoseconds)

        // Calculate a 3% jitter range
        let jitterRange = (backoffNanoseconds / 100) * 3
        // Pick a random element from the range +/- jitter range.
        let jitter: Duration = .nanoseconds((-jitterRange...jitterRange).randomElement()!)
        let jitteredBackoff = backoff + jitter
        return jitteredBackoff
    }
}
