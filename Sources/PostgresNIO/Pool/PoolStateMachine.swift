#if swift(>=5.7)
import NIOCore

protocol PooledConnection: AnyObject {
    associatedtype ID: Hashable

    var id: ID { get }

    var eventLoop: EventLoop { get }
}

protocol ConnectionIDGeneratorProtocol {
    associatedtype ID: Hashable

    func next() -> ID
}

protocol ConnectionRequest {
    associatedtype ID: Hashable

    var id: ID { get }

    var preferredEventLoop: EventLoop? { get }

    var deadline: NIODeadline { get }
}

struct PoolStateMachine<
    Connection: PooledConnection,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    ConnectionID,
    Request: ConnectionRequest,
    RequestID
> where Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, RequestID == Request.ID {

    struct Action {
        let request: RequestAction
        let connection: ConnectionAction

        init(request: RequestAction, connection: ConnectionAction) {
            self.request = request
            self.connection = connection
        }

        static func none() -> Action { Action(request: .none, connection: .none) }
    }

    enum ConnectionAction {
        struct Shutdown {
            struct ConnectionToClose {
                var cancelIdleTimer: Bool
                var cancelPingPongTimer: Bool
                var connection: Connection
            }

            var connections: [ConnectionToClose]
            var backoffTimersToCancel: [ConnectionID]

            init() {
                self.connections = []
                self.backoffTimersToCancel = []
            }
        }

        case createConnection(ConnectionRequest)
        case scheduleBackoffTimer(Connection.ID, backoff: TimeAmount, on: EventLoop)

        case schedulePingTimer(Connection.ID, on: EventLoop)
        case cancelPingTimer(Connection.ID)
        case runPingPong(Connection)

        case schedulePingAndIdleTimeoutTimer(Connection.ID, on: EventLoop)
        case cancelPingAndIdleTimeoutTimer(Connection.ID)
        case cancelIdleTimeoutTimer(Connection.ID)

        case closeConnection(Connection, cancelPingPongTimer: Bool)
        case shutdown(Shutdown)
        case shutdownComplete(EventLoopPromise<Void>)

        case none
    }

    enum RequestAction {
        case leaseConnection(Request, Connection, cancelTimeout: Bool)

        case failRequest(Request, Error, cancelTimeout: Bool)
        case failRequestsAndCancelTimeouts([Request], Error)

        case scheduleRequestTimeout(for: Request, on: EventLoop)

        case none
    }

    private enum PoolState {
        case running
        case shuttingDown(graceful: Bool, promise: EventLoopPromise<Void>)
        case shutDown
    }

    struct ConnectionRequest: Equatable {
        var eventLoop: any EventLoop
        var connectionID: ConnectionID

        static func ==(lhs: Self, rhs: Self) -> Bool {
            lhs.connectionID == rhs.connectionID && lhs.eventLoop === rhs.eventLoop
        }
    }

    private let configuration: PostgresConnectionPoolConfiguration
    private let generator: ConnectionIDGenerator
    private let eventLoops: [any EventLoop]
    private let eventLoopGroup: any EventLoopGroup

    private var connections: [EventLoopID: EventLoopConnections]
    private var requestQueue: RequestQueue
    private var poolState: PoolState = .running

    private var failedConsecutiveConnectionAttempts: Int = 0
    /// the error from the last connection creation
    private var lastConnectFailure: Error?

    init(
        configuration: PostgresConnectionPoolConfiguration,
        generator: ConnectionIDGenerator,
        eventLoopGroup: any EventLoopGroup
    ) {
        self.configuration = configuration
        self.generator = generator
        self.eventLoops = Array(eventLoopGroup.makeIterator())
        self.eventLoopGroup = eventLoopGroup
        self.connections = [:]
        self.connections.reserveCapacity(self.eventLoops.count)
        self.requestQueue = .init(eventLoopGroup: eventLoopGroup)

        let minimumConnectionsPerEL = configuration.minimumConnectionCount / self.eventLoops.count
        var additionalMinimumConnections = configuration.minimumConnectionCount % self.eventLoops.count

        let maximumConnectionsPerELSoftLimit = configuration.maximumConnectionSoftLimit / self.eventLoops.count
        var additionalMaximumConnectionsSoftLimit = configuration.maximumConnectionSoftLimit % self.eventLoops.count

        let maximumConnectionsPerELHardLimit = configuration.maximumConnectionHardLimit / self.eventLoops.count
        var additionalMaximumConnectionsHardLimit = configuration.maximumConnectionHardLimit % self.eventLoops.count

        for eventLoop in self.eventLoops {
            let eventLoopID = EventLoopID(eventLoop)

            let additionalMinimumConnection: Int
            if additionalMinimumConnections > 0 {
                additionalMinimumConnection = 1
                additionalMinimumConnections -= 1
            } else {
                additionalMinimumConnection = 0
            }

            let additionalMaximumConnectionSoftLimit: Int
            if additionalMaximumConnectionsSoftLimit > 0 {
                additionalMaximumConnectionSoftLimit = 1
                additionalMaximumConnectionsSoftLimit -= 1
            } else {
                additionalMaximumConnectionSoftLimit = 0
            }

            let additionalMaximumConnectionHardLimit: Int
            if additionalMaximumConnectionsHardLimit > 0 {
                additionalMaximumConnectionHardLimit = 1
                additionalMaximumConnectionsHardLimit -= 1
            } else {
                additionalMaximumConnectionHardLimit = 0
            }

            let connection = EventLoopConnections(
                eventLoop: eventLoop,
                generator: generator,
                minimumConcurrentConnections: minimumConnectionsPerEL + additionalMinimumConnection,
                maximumConcurrentConnectionSoftLimit: maximumConnectionsPerELSoftLimit + additionalMaximumConnectionSoftLimit,
                maximumConcurrentConnectionHardLimit: maximumConnectionsPerELHardLimit + additionalMaximumConnectionHardLimit
            )

            self.connections[eventLoopID] = connection
        }
    }

    mutating func refillConnections() -> [ConnectionRequest] {
        var request = [ConnectionRequest]()
        request.reserveCapacity(self.configuration.minimumConnectionCount)

        self.connections = self.connections.mapValues { connections in
            var connections = connections
            connections.refillConnections(&request)
            return connections
        }

        return request
    }

    mutating func leaseConnection(_ request: Request) -> Action {
        switch self.poolState {
        case .running:
            break

        case .shuttingDown, .shutDown:
            return .init(
                request: .failRequest(request, PSQLError.poolClosed, cancelTimeout: false),
                connection: .none
            )
        }

        // check if the preferredEL has an idle connection
        if let preferredEL = request.preferredEventLoop {
            if let connection = self.connections[preferredEL.id]!.leaseConnection() {
                return .init(
                    request: .leaseConnection(request, connection, cancelTimeout: false),
                    connection: .cancelPingTimer(connection.id)
                )
            }
        }

        var soonAvailable: UInt16 = 0

        // check if any other EL has an idle connection
        for index in RandomStartIndexIterator(self.connections) {
            var (key, connections) = self.connections[index]
            switch connections.leaseConnectionOrSoonAvailableConnectionCount() {
            case .startingCount(let count):
                soonAvailable += count
            case .leasedConnection(let connection):
                self.connections[key] = connections
                return .init(
                    request: .leaseConnection(request, connection, cancelTimeout: false),
                    connection: .cancelPingTimer(connection.id)
                )
            }
        }

        // we tried everything. there is no connection available. now we must check, if and where we
        // can create further connections. but first we must enqueue the new request

        self.requestQueue.queue(request)

        let requestAction: RequestAction = .scheduleRequestTimeout(for: request, on: request.preferredEventLoop ?? self.eventLoopGroup.any())

        if soonAvailable >= self.requestQueue.count {
            // if more connections will be soon available then we have waiters, we don't need to
            // create further new connections.
            return .init(
                request: requestAction,
                connection: .none
            )
        }

        // create new demand connection

        if let preferredEL = request.preferredEventLoop {
            if let request = self.connections[preferredEL.id]!.createNewDemandConnectionIfPossible() {
                return .init(
                    request: requestAction,
                    connection: .createConnection(request)
                )
            }
        }

        // check if any other EL has an idle connection
        for index in RandomStartIndexIterator(self.connections) {
            var (key, connections) = self.connections[index]
            if let request = connections.createNewDemandConnectionIfPossible() {
                self.connections[key] = connections
                return .init(
                    request: requestAction,
                    connection: .createConnection(request)
                )
            }
        }

        // create new overflow connections

        if let preferredEL = request.preferredEventLoop {
            if let request = self.connections[preferredEL.id]!.createNewOverflowConnectionIfPossible() {
                return .init(
                    request: requestAction,
                    connection: .createConnection(request)
                )
            }
        }

        // check if any other EL has an idle connection
        for index in RandomStartIndexIterator(self.connections) {
            var (key, connections) = self.connections[index]
            if let request = connections.createNewOverflowConnectionIfPossible() {
                self.connections[key] = connections
                return .init(
                    request: requestAction,
                    connection: .createConnection(request)
                )
            }
        }

        // no new connections allowed:
        return .init(request: requestAction, connection: .none)
    }

    mutating func releaseConnection(_ connection: Connection) -> Action {
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, idleContext) = self.connections[eventLoopID]!.releaseConnection(connection.id)

        return self.handleIdleConnection(eventLoopID, index: index, idleContext: idleContext)
    }

    mutating func cancelRequest(id: RequestID) -> Action {
        guard let request = self.requestQueue.remove(id) else {
            return .none()
        }

        return .init(
            request: .failRequest(request, PSQLError.queryCancelled, cancelTimeout: true),
            connection: .none
        )
    }

    mutating func timeoutRequest(id: RequestID) -> Action {
        guard let request = self.requestQueue.remove(id) else {
            return .none()
        }

        return .init(
            request: .failRequest(request, PSQLError.timeoutError, cancelTimeout: false),
            connection: .none
        )
    }

    mutating func connectionEstablished(_ connection: Connection) -> Action {
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, idleContext) = self.connections[eventLoopID]!.newConnectionEstablished(connection)

        return self.handleIdleConnection(eventLoopID, index: index, idleContext: idleContext)
    }

    mutating func connectionEstablishFailed(_ error: any Error, for request: ConnectionRequest) -> Action {
        self.failedConsecutiveConnectionAttempts += 1
        self.lastConnectFailure = error

        let eventLoopID = EventLoopID(request.eventLoop)
        let eventLoop = self.connections[eventLoopID]!.backoffNextConnectionAttempt(request.connectionID)
        let backoff = Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
        return .init(request: .none, connection: .scheduleBackoffTimer(request.connectionID, backoff: backoff, on: eventLoop))
    }

    mutating func connectionCreationBackoffDone(_ connectionID: ConnectionID, on eventLoop: EventLoop) -> Action {
        let eventLoopID = EventLoopID(eventLoop)

        let soonAvailable = self.connections.values.reduce(0, { $0 + $1.soonAvailable })
        let retry = (soonAvailable - 1) < self.requestQueue.count

        switch self.connections[eventLoopID]!.backoffDone(connectionID, retry: retry) {
        case .createConnection(let request):
            return .init(request: .none, connection: .createConnection(request))
        case .cancelIdleTimeoutTimer(let connectionID):
            return .init(request: .none, connection: .cancelIdleTimeoutTimer(connectionID))
        case .none:
            return .none()
        }
    }

    mutating func connectionPingTimerTriggered(_ connectionID: ConnectionID, on eventLoop: EventLoop) -> Action {
        precondition(self.requestQueue.isEmpty)

        guard let connection = self.connections[.init(eventLoop)]!.pingPongIfIdle(connectionID) else {
            return .none()
        }
        return .init(request: .none, connection: .runPingPong(connection))
    }

    mutating func connectionPingPongDone(_ connection: Connection) -> Action {
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, idleContext) = self.connections[eventLoopID]!.pingPongDone(connection.id)
        return self.handleIdleConnection(eventLoopID, index: index, idleContext: idleContext)
    }

    mutating func connectionIdleTimerTriggered(_ connectionID: ConnectionID, on eventLoop: EventLoop) -> Action {
        precondition(self.requestQueue.isEmpty)

        guard let (connection, cancelPingPongTimer) = self.connections[.init(eventLoop)]!.closeConnectionIfIdle(connectionID) else {
            return .none()
        }
        return .init(request: .none, connection: .closeConnection(connection, cancelPingPongTimer: cancelPingPongTimer))
    }

    mutating func connectionClosed(_ connection: Connection) -> Action {
        fatalError()
    }

    struct CleanupAction {
        struct ConnectionToDrop {
            var connection: Connection
            var pingTimer: Bool
            var idleTimer: Bool
        }

        var connections: [ConnectionToDrop]
        var requests: [Request]
    }

    mutating func gracefulShutdown(_ promise: EventLoopPromise<Void>) -> Action {
        fatalError()
    }

    mutating func forceShutdown(_ promise: EventLoopPromise<Void>) -> Action {
        switch self.poolState {
        case .running:
            self.poolState = .shuttingDown(graceful: true, promise: promise)
            var shutdown = ConnectionAction.Shutdown()
            for key in self.connections.keys {
                self.connections[key]!.shutdown(&shutdown)
            }
            return .init(
                request: .failRequestsAndCancelTimeouts(self.requestQueue.removeAll(), PSQLError.poolClosed),
                connection: .shutdown(shutdown)
            )

        case .shuttingDown(_, let existingPromise):
            existingPromise.futureResult.cascade(to: promise)
            return .none()

        case .shutDown:
            return .init(request: .none, connection: .shutdownComplete(promise))

        }
    }

    private mutating func handleIdleConnection(_ eventLoopID: EventLoopID, index: Int, idleContext: EventLoopConnections.IdleConnectionContext) -> Action {
        if let request = self.requestQueue.pop(for: eventLoopID) {
            let connection = self.connections[eventLoopID]!.leaseConnection(at: index)
            return .init(
                request: .leaseConnection(request, connection, cancelTimeout: true),
                connection: .none
            )
        }

        switch idleContext.use {
        case .persisted:
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            return .init(request: .none, connection: .schedulePingTimer(connectionID, on: idleContext.eventLoop))
        case .demand:
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            if idleContext.hasBecomeIdle {
                return .init(
                    request: .none,
                    connection: .schedulePingAndIdleTimeoutTimer(connectionID, on: idleContext.eventLoop)
                )
            } else {
                return .init(
                    request: .none,
                    connection: .schedulePingTimer(connectionID, on: idleContext.eventLoop)
                )
            }

        case .overflow:
            let connection = self.connections[eventLoopID]!.closeConnection(at: index)
            return .init(request: .none, connection: .closeConnection(connection, cancelPingPongTimer: false))
        }
    }
}

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
    static func calculateBackoff(failedAttempt attempts: Int) -> TimeAmount {
        // Our backoff formula is: 100ms * 1.25^(attempts - 1) that is capped of at 1minute
        // This means for:
        //   -  1 failed attempt :  100ms
        //   -  5 failed attempts: ~300ms
        //   - 10 failed attempts: ~930ms
        //   - 15 failed attempts: ~2.84s
        //   - 20 failed attempts: ~8.67s
        //   - 25 failed attempts: ~26s
        //   - 29 failed attempts: ~60s (max out)

        let start = Double(TimeAmount.milliseconds(100).nanoseconds)
        let backoffNanosecondsDouble = start * pow(1.25, Double(attempts - 1))

        // Cap to 60s _before_ we convert to Int64, to avoid trapping in the Int64 initializer.
        let backoffNanoseconds = Int64(min(backoffNanosecondsDouble, Double(TimeAmount.seconds(60).nanoseconds)))

        let backoff = TimeAmount.nanoseconds(backoffNanoseconds)

        // Calculate a 3% jitter range
        let jitterRange = (backoff.nanoseconds / 100) * 3
        // Pick a random element from the range +/- jitter range.
        let jitter: TimeAmount = .nanoseconds((-jitterRange...jitterRange).randomElement()!)
        let jitteredBackoff = backoff + jitter
        return jitteredBackoff
    }

}

struct EventLoopID: Hashable {
    var objectID: ObjectIdentifier

    init(_ eventLoop: EventLoop) {
        self.objectID = ObjectIdentifier(eventLoop)
    }
}

extension EventLoop {
    var id: EventLoopID { .init(self) }
}

struct RandomStartIndexIterator<Collection: Swift.Collection>: Sequence, IteratorProtocol {
    private let collection: Collection
    private let startIndex: Collection.Index?
    private var index: Collection.Index?

    init(_ collection: Collection) {
        self.collection = collection
        self.startIndex = collection.indices.randomElement()
        self.index = self.startIndex
    }

    mutating func next() -> Collection.Index? {
        guard let index = self.index else { return nil }
        defer {
            let nextIndex = self.collection.index(after: index)
            if nextIndex == self.collection.endIndex {
                self.index = self.collection.startIndex
            } else {
                self.index = nextIndex
            }
            if self.index == self.startIndex {
                self.index = nil
            }
        }
        return index
    }

    func makeIterator() -> RandomStartIndexIterator<Collection> {
        self
    }
}

#endif
