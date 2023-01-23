#if swift(>=5.7)
import NIOCore

protocol PooledConnection {
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

@available(macOS 13.0, iOS 16.0, *)
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
        enum IsShutdown: Equatable {
            case yes(unclean: Bool)
            case no
        }

        case createConnection(ConnectionRequest)
        case scheduleBackoffTimer(Connection.ID, backoff: TimeAmount, on: EventLoop)

        case schedulePingTimer(Connection.ID, on: EventLoop)
        case cancelPingTimer(Connection.ID)

        case schedulePingAndIdleTimeoutTimer(Connection.ID, on: EventLoop)
        case cancelPingAndIdleTimeoutTimer(Connection.ID)
        case cancelIdleTimeoutTimer(Connection.ID)

        case closeConnection(Connection)

        case none
    }

    enum RequestAction {
        case leaseConnection(Request, Connection, cancelTimeout: Bool)

        case failRequest(Request, Error, cancelTimeout: Bool)
        case failRequestsAndCancelTimeouts([Request], Error)

        case scheduleRequestTimeout(for: Request, on: EventLoop)

        case none
    }

    private enum PoolState: Equatable {
        case running
        case shuttingDown(graceful: Bool)
        case shutDown
    }

    struct ConnectionRequest {
        var eventLoop: any EventLoop
        var connectionID: ConnectionID
    }

    private let configuration: PostgresConnectionPoolConfiguration
    private let generator: ConnectionIDGenerator
    private let eventLoops: [any EventLoop]

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

    mutating func requestConnection(_ request: Request) -> Action {
        if let preferredEL = request.preferredEventLoop {
            if let connection = self.connections[preferredEL.id]!.leaseConnection() {
                return .init(
                    request: .leaseConnection(request, connection, cancelTimeout: false),
                    connection: .cancelPingTimer(connection.id)
                )
            }
        }

        let breakIndex = self.connections.indices.randomElement()!

        var index: Dictionary<EventLoopID, EventLoopConnections>.Index

        repeat {
            let nextIndex = self.connections.index(after: breakIndex)
            if nextIndex == self.connections.endIndex {
                index = self.connections.startIndex
            } else {
                index = nextIndex
            }

            var (key, connections) = self.connections[index]
            if let connection = connections.leaseConnection() {
                self.connections[key] = connections
                return .init(
                    request: .leaseConnection(request, connection, cancelTimeout: false),
                    connection: .cancelPingTimer(connection.id)
                )
            }
        } while index != breakIndex

        // we tried everything. there is no connection available. now we must check, if and where we
        // can create further connections. but first we must enqueue the new request

        self.requestQueue.queue(request)

        if let preferredEL = request.preferredEventLoop {
            if let connection = self.connections[preferredEL.id]!.createNewConnection() {

                return .init(
                    request: .none,
                    connection: .createConnection(connection)
                )
            }
        }


    }

    mutating func cancelRequest(id: RequestID) -> Action {
        fatalError()
    }

    mutating func timeoutRequest(id: RequestID) -> Action {
        fatalError()
    }

    mutating func connectionEstablished(_ connection: Connection) -> Action {
        let eventLoopID = EventLoopID(connection.eventLoop)
        let (index, idleContext) = self.connections[eventLoopID]!.newConnectionEstablished(connection)

        if let request = self.requestQueue.pop(for: .init(connection.eventLoop)) {
            let connection = self.connections[eventLoopID]!.leaseConnection(at: index)
            return .init(
                request: .leaseConnection(request, connection, cancelTimeout: true),
                connection: .none
            )
        }

        switch idleContext.use {
        case .persisted:
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            return .init(request: .none, connection: .schedulePingTimer(connectionID, on: connection.eventLoop))
        case .overflow:
            let connectionID = self.connections[eventLoopID]!.parkConnection(at: index)
            return .init(request: .none, connection: .schedulePingAndIdleTimeoutTimer(connectionID, on: connection.eventLoop))
        case .oneof:
            let connection = self.connections[eventLoopID]!.closeConnection(at: index)
            return .init(request: .none, connection: .closeConnection(connection))
        }
    }

    mutating func connectionEstablishFailed(_ error: any Error, for request: ConnectionRequest) -> Action {
        self.failedConsecutiveConnectionAttempts += 1
        self.lastConnectFailure = error

        let eventLoopID = EventLoopID(request.eventLoop)
        let eventLoop = self.connections[eventLoopID]!.backoffNextConnectionAttempt(request.connectionID)
        let backoff = Self.calculateBackoff(failedAttempt: self.failedConsecutiveConnectionAttempts)
        return .init(request: .none, connection: .scheduleBackoffTimer(request.connectionID, backoff: backoff, on: eventLoop))
    }

    mutating func connectionCreationBackoffDone(_ connectionID: ConnectionID) -> Action {
        fatalError()
    }

    mutating func connectionPingTimerTriggered(_ connectionID: ConnectionID) -> Action {
        fatalError()
    }

    mutating func connectionClosed() -> Action {
        fatalError()
    }

    mutating func shutdown() -> Action {
        fatalError()
    }
}

@available(macOS 13.0, iOS 16.0, *)
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

#endif
