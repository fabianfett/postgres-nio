import NIOCore
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
    var maxConsecutivePicksFromEventLoopQueue: UInt8 = 16

    @usableFromInline
    var keepAlive: Bool = false
}

@usableFromInline
struct RequestCollection<Element: ConnectionRequestProtocol>: Sequence {
    @usableFromInline
    enum Base {
        case none(reserveCapacity: Int)
        case one(Element, reserveCapacity: Int)
        case n([Element])
    }

    @usableFromInline
    private(set) var base: Base

    @inlinable
    init() {
        self.base = .none(reserveCapacity: 0)
    }

    @inlinable
    init(_ element: Element) {
        self.base = .one(element, reserveCapacity: 1)
    }

    @inlinable
    init(_ collection: some Collection<Element>) {
        switch collection.count {
        case 0:
            self.base = .none(reserveCapacity: 0)
        case 1:
            self.base = .one(collection.first!, reserveCapacity: 0)
        default:
            self.base = .n(Array(collection))
        }
    }

    @usableFromInline
    var count: Int {
        switch self.base {
        case .none:
            return 0
        case .one:
            return 1
        case .n(let array):
            return array.count
        }
    }

    @inlinable
    var first: Element? {
        switch self.base {
        case .none:
            return nil
        case .one(let element, _):
            return element
        case .n(let array):
            return array.first
        }
    }

    @usableFromInline
    var isEmpty: Bool {
        switch self.base {
        case .none:
            return true
        case .one, .n:
            return false
        }
    }

    @inlinable
    mutating func reserveCapacity(_ minimumCapacity: Int) {
        switch self.base {
        case .none(let reservedCapacity):
            self.base = .none(reserveCapacity: Swift.max(reservedCapacity, minimumCapacity))
        case .one(let element, let reservedCapacity):
            self.base = .one(element, reserveCapacity: Swift.max(reservedCapacity, minimumCapacity))
        case .n(var array):
            array.reserveCapacity(minimumCapacity)
        }
    }

    @inlinable
    mutating func append(_ element: Element) {
        switch self.base {
        case .none(let reserveCapacity):
            self.base = .one(element, reserveCapacity: reserveCapacity)
        case .one(let existing, let reserveCapacity):
            var new = [Element]()
            new.reserveCapacity(reserveCapacity)
            new.append(existing)
            new.append(element)
            self.base = .n(new)
        case .n(var existing):
            self.base = .none(reserveCapacity: 0) // prevent CoW
            existing.append(element)
            self.base = .n(existing)
        }
    }

    @inlinable
    func makeIterator() -> Iterator {
        Iterator(self)
    }

    @usableFromInline
    struct Iterator: IteratorProtocol {
        @usableFromInline private(set) var index: Int = 0
        @usableFromInline private(set) var backing: RequestCollection<Element>

        @inlinable
        init(_ backing: RequestCollection<Element>) {
            self.backing = backing
        }

        @inlinable
        mutating func next() -> Element? {
            switch self.backing.base {
            case .none:
                return nil
            case .one(let element, _):
                if self.index == 0 {
                    self.index += 1
                    return element
                }
                return nil

            case .n(let array):
                if self.index < array.endIndex {
                    defer { self.index += 1}
                    return array[self.index]
                }
                return nil
            }
        }
    }
}

@usableFromInline
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

        case createConnection(ConnectionRequest)
        case scheduleBackoffTimer(Connection.ID, backoff: TimeAmount, on: EventLoop)

        case scheduleKeepAliveTimer(Connection.ID, on: EventLoop)
        case cancelKeepAliveTimer(Connection.ID)
        case runKeepAlive(Connection)

        case scheduleIdleTimeoutTimer(Connection.ID, on: EventLoop)
        case cancelIdleTimeoutTimer(Connection.ID)

        case scheduleKeepAliveAndIdleTimeoutTimer(Connection.ID, on: EventLoop)
        case cancelKeepAliveAndIdleTimeoutTimer(Connection.ID)

        case closeConnection(Connection, cancelKeepAliveTimer: Bool)
        case shutdown(Shutdown)
        case shutdownComplete(EventLoopPromise<Void>)

        case none

        @usableFromInline
        static func ==(lhs: Self, rhs: Self) -> Bool {
            switch (lhs, rhs) {
            case (.createConnection(let lhs), .createConnection(let rhs)):
                return lhs == rhs
            case (.scheduleBackoffTimer(let lhsConnID, let lhsBackoff, on: let lhsEL), .scheduleBackoffTimer(let rhsConnID, let rhsBackoff, on: let rhsEL)):
                return lhsConnID == rhsConnID && lhsBackoff == rhsBackoff && lhsEL === rhsEL
            case (.scheduleKeepAliveTimer(let lhsConnID, on: let lhsEL), .scheduleKeepAliveTimer(let rhsConnID, on: let rhsEL)):
                return lhsConnID == rhsConnID && lhsEL === rhsEL
            case (.cancelKeepAliveTimer(let lhs), .cancelKeepAliveTimer(let rhs)):
                return lhs == rhs
            case (.runKeepAlive(let lhs), .runKeepAlive(let rhs)):
                return lhs === rhs
            case (.scheduleKeepAliveAndIdleTimeoutTimer(let lhsConnID, on: let lhsEL), .scheduleKeepAliveAndIdleTimeoutTimer(let rhsConnID, on: let rhsEL)):
                return lhsConnID == rhsConnID && lhsEL === rhsEL
            case (.cancelKeepAliveAndIdleTimeoutTimer(let lhs), .cancelKeepAliveAndIdleTimeoutTimer(let rhs)):
                return lhs == rhs
            case (.cancelIdleTimeoutTimer(let lhs), .cancelIdleTimeoutTimer(let rhs)):
                return lhs == rhs
            case (.closeConnection(let lhsConn, cancelKeepAliveTimer: let lhsCancel), .closeConnection(let rhsConn, cancelKeepAliveTimer: let rhsCancel)):
                return lhsConn === rhsConn && lhsCancel == rhsCancel
            case (.shutdown(let lhs), .shutdown(let rhs)):
                return lhs == rhs
            case (.shutdownComplete(let lhs), .shutdownComplete(let rhs)):
                return lhs.futureResult === rhs.futureResult
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
        case shuttingDown(graceful: Bool, promise: EventLoopPromise<Void>)
        case shutDown
    }

    @usableFromInline
    struct ConnectionRequest: Equatable {
        @usableFromInline var eventLoop: any EventLoop
        @usableFromInline var connectionID: ConnectionID

        @usableFromInline
        init(eventLoop: any EventLoop, connectionID: ConnectionID) {
            self.eventLoop = eventLoop
            self.connectionID = connectionID
        }

        @usableFromInline
        static func ==(lhs: Self, rhs: Self) -> Bool {
            lhs.connectionID == rhs.connectionID && lhs.eventLoop === rhs.eventLoop
        }
    }

    @usableFromInline let configuration: PoolConfiguration
    @usableFromInline let generator: ConnectionIDGenerator
    @usableFromInline let eventLoops: [any EventLoop]
    @usableFromInline let eventLoopGroup: any EventLoopGroup

    @usableFromInline
    private(set) var connections: [EventLoopID: EventLoopConnections]
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
        generator: ConnectionIDGenerator,
        eventLoopGroup: any EventLoopGroup
    ) {
        self.configuration = configuration
        self.generator = generator
        self.eventLoops = Array(eventLoopGroup.makeIterator())
        self.eventLoopGroup = eventLoopGroup
        self.connections = [:]
        self.connections.reserveCapacity(self.eventLoops.count)
        self.requestQueue = RequestQueue()

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
                maximumConcurrentConnectionHardLimit: maximumConnectionsPerELHardLimit + additionalMaximumConnectionHardLimit,
                keepAliveReducesAvailableStreams: true
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

    @inlinable
    mutating func leaseConnection(_ request: Request) -> Action {
        func connectionActionForLease(_ connectionID: ConnectionID, info: ConnectionLeasedInfo) -> ConnectionAction {
            switch (self.configuration.keepAlive, info.use, info.wasIdle) {
            case (_, .demand, false), (_, .persisted, false):
                return .none
            case (true, .demand, true):
                return .cancelKeepAliveAndIdleTimeoutTimer(connectionID)
            case (false, .demand, true):
                return .cancelIdleTimeoutTimer(connectionID)
            case (true, .persisted, true):
                return .cancelKeepAliveTimer(connectionID)
            case (false, .persisted, true):
                return .none
            case (_, .overflow, _):
                preconditionFailure("Overflow connections should never be available on fast turn around")
            }
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
        for index in RandomStartIndexIterator(self.connections) {
            var (key, connections) = self.connections[index]
            switch connections.leaseConnectionOrSoonAvailableConnectionCount() {
            case .startingCount(let count):
                soonAvailable += count
            case .leasedConnection(let connection, let info):
                self.connections[key] = connections
                return .init(
                    request: .leaseConnection(.init(request), connection),
                    connection: connectionActionForLease(connection.id, info: info)
                )
            }
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

        // check if any other EL has more space
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

        self.cacheNoMoreConnectionsAllowed = true

        // no new connections allowed:
        return .init(request: requestAction, connection: .none)
    }

    @inlinable
    mutating func releaseConnection(_ connection: Connection, streams: UInt16) -> Action {
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, context) = self.connections[eventLoopID]!.releaseConnection(connection.id, streams: streams)
        return self.handleAvailableConnection(eventLoopID, index: index, availableContext: context)
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
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, context) = self.connections[eventLoopID]!.newConnectionEstablished(connection, maxStreams: maxStreams)
        return self.handleAvailableConnection(eventLoopID, index: index, availableContext: context)
    }

    @inlinable
    mutating func connectionEstablishFailed(_ error: Error, for request: ConnectionRequest) -> Action {
        self.failedConsecutiveConnectionAttempts += 1

        let eventLoopID = EventLoopID(request.eventLoop)
        let eventLoop = self.connections[eventLoopID]!.backoffNextConnectionAttempt(request.connectionID)
        let backoff = Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
        return .init(request: .none, connection: .scheduleBackoffTimer(request.connectionID, backoff: backoff, on: eventLoop))
    }

    @inlinable
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

    @inlinable
    mutating func connectionKeepAliveTimerTriggered(_ connectionID: ConnectionID, on eventLoop: EventLoop) -> Action {
        precondition(self.configuration.keepAlive)
        precondition(self.requestQueue.isEmpty)

        guard let connection = self.connections[.init(eventLoop)]!.keepAliveIfIdle(connectionID) else {
            return .none()
        }
        return .init(request: .none, connection: .runKeepAlive(connection))
    }

    @inlinable
    mutating func connectionKeepAliveDone(_ connection: Connection) -> Action {
        precondition(self.configuration.keepAlive)
        let eventLoopID = EventLoopID(connection.eventLoop)
        guard let (index, context) = self.connections[eventLoopID]!.keepAliveSucceeded(connection.id) else {
            return .none()
        }
        return self.handleAvailableConnection(eventLoopID, index: index, availableContext: context)
    }

    @inlinable
    mutating func connectionIdleTimerTriggered(_ connectionID: ConnectionID, on eventLoop: EventLoop) -> Action {
        precondition(self.requestQueue.isEmpty)

        guard let (connection, cancelKeepAliveTimer) = self.connections[.init(eventLoop)]!.closeConnectionIfIdle(connectionID) else {
            return .none()
        }

        self.cacheNoMoreConnectionsAllowed = false

        return .init(request: .none, connection: .closeConnection(connection, cancelKeepAliveTimer: cancelKeepAliveTimer))
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
                request: .failRequests(self.requestQueue.removeAll(), PoolError.poolShutdown),
                connection: .shutdown(shutdown)
            )

        case .shuttingDown(_, let existingPromise):
            existingPromise.futureResult.cascade(to: promise)
            return .none()

        case .shutDown:
            return .init(request: .none, connection: .shutdownComplete(promise))

        }
    }

    @inlinable
    /*private*/ mutating func handleAvailableConnection(
        _ eventLoopID: EventLoopID,
        index: Int,
        availableContext: EventLoopConnections.AvailableConnectionContext
    ) -> Action {
        // this connection was busy before
        var requests = RequestCollection<Request>()
        requests.reserveCapacity(Int(availableContext.info.availableStreams))
        for _ in 0..<availableContext.info.availableStreams {
            if let request = self.requestQueue.pop(for: eventLoopID) {
                requests.append(request)
            } else {
                break
            }
        }
        if !requests.isEmpty {
            let (connection, _) = self.connections[eventLoopID]!.leaseConnection(at: index, streams: UInt16(requests.count))
            return .init(
                request: .leaseConnection(requests, connection),
                connection: .none
            )
        }

        func makeIdleConnectionAction(for connectionID: ConnectionID, scheduleTimeout: Bool) -> ConnectionAction {
            switch (scheduleTimeout, self.configuration.keepAlive) {
            case (false, false):
                return .none
            case (true, false):
                return .scheduleIdleTimeoutTimer(connectionID, on: availableContext.eventLoop)
            case (false, true):
                return .scheduleKeepAliveTimer(connectionID, on: availableContext.eventLoop)
            case (true, true):
                return .scheduleKeepAliveAndIdleTimeoutTimer(connectionID, on: availableContext.eventLoop)
            }
        }

        switch (availableContext.use, availableContext.info) {
        case (.persisted, .idle):
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            return .init(
                request: .none,
                connection: makeIdleConnectionAction(for: connectionID, scheduleTimeout: false)
            )

        case (.demand, .idle):
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            guard case .idle(availableStreams: _, let newIdle) = availableContext.info else {
                preconditionFailure()
            }
            return .init(
                request: .none,
                connection: makeIdleConnectionAction(for: connectionID, scheduleTimeout: newIdle)
            )

        case (.overflow, .idle):
            let connection = self.connections[eventLoopID]!.closeConnection(at: index)
            return .init(request: .none, connection: .closeConnection(connection, cancelKeepAliveTimer: false))

        case (_, .leased):
            return .none()
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
    @usableFromInline
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

@usableFromInline
struct EventLoopID: Hashable {
    @usableFromInline
    var objectID: ObjectIdentifier

    @inlinable
    init(_ eventLoop: EventLoop) {
        self.objectID = ObjectIdentifier(eventLoop)
    }
}

extension EventLoop {
    @inlinable
    var id: EventLoopID { .init(self) }
}

@usableFromInline
struct RandomStartIndexIterator<Collection: Swift.Collection>: Sequence, IteratorProtocol {
    @usableFromInline let collection: Collection
    @usableFromInline let startIndex: Collection.Index?
    @usableFromInline private(set) var index: Collection.Index?

    @inlinable
    init(_ collection: Collection) {
        self.collection = collection
        self.startIndex = collection.indices.randomElement()
        self.index = self.startIndex
    }

    @inlinable
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

    @inlinable
    func makeIterator() -> RandomStartIndexIterator<Collection> {
        self
    }
}
