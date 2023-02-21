import Atomics
import NIOCore

extension PoolStateMachine {

    private struct ConnectionState {
        enum State {
            enum KeepAlive: Equatable {
                case notRunning
                case running(Bool)

                var usedStreams: Int {
                    switch self {
                    case .notRunning, .running(false):
                        return 0
                    case .running(true):
                        return 1
                    }
                }
            }

            /// The pool is creating a connection. Valid transitions are to: `.backingOff`, `.idle`, and `.closed`
            case starting
            /// The pool is waiting to retry establishing a connection. Valid transitions are to: `.closed`.
            /// This means, the connection can be removed from the connections without cancelling external
            /// state. The connection state can then be replaced by a new one.
            case backingOff
            /// The connection is `idle` and ready to execute a new query. Valid transitions to: `.pingpong`, `.leased`,
            /// `.closing` and `.closed`
            case idle(Connection, maxStreams: Int, keepAlive: KeepAlive)
            /// The connection is leased and executing a query. Valid transitions to: `.idle` and `.closed`
            case leased(Connection, usedStreams: Int, maxStreams: Int, keepAlive: KeepAlive)
            /// The connection is closing. Valid transitions to: `.closed`
            case closing(Connection)
            /// The connection is closed. Final state.
            case closed
        }

        let id: Connection.ID

        private var state: State

        init(id: Connection.ID) {
            self.id = id
            self.state = .starting
        }

        var isIdle: Bool {
            switch self.state {
            case .idle(_, _, .notRunning):
                return true
            case .idle(_, _, .running):
                return false
            case .backingOff, .starting, .closed, .closing, .leased:
                return false
            }
        }

        var isAvailable: Bool {
            switch self.state {
            case .idle(_, let maxStreams, .running(true)):
                return maxStreams > 1
            case .idle(_, let maxStreams, let keepAlive):
                return keepAlive.usedStreams < maxStreams
            case .leased(_, let usedStreams, let maxStreams, let keepAlive):
                return usedStreams + keepAlive.usedStreams < maxStreams
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .closed, .closing, .idle:
                return false
            }
        }

        var isIdleOrRunningKeepAlive: Bool {
            switch self.state {
            case .idle:
                return true
            case .backingOff, .starting, .closed, .closing, .leased:
                return false
            }
        }

        var isConnected: Bool {
            switch self.state {
            case .idle, .leased:
                return true
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        mutating func connected(_ connection: Connection, maxStreams: Int) -> ConnectionAvailableInfo {
            switch self.state {
            case .starting:
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .notRunning)
                return .idle(availableStreams: maxStreams, newIdle: true)
            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        /// The connection failed to start
        mutating func failedToConnect() {
            switch self.state {
            case .starting:
                self.state = .backingOff
            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func retryConnect() {
            switch self.state {
            case .backingOff:
                self.state = .starting
            case .starting, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func lease(streams newLeasedStreams: Int = 1) -> Connection {
            switch self.state {
            case .idle(let connection, let maxStreams, let keepAlive):
                precondition(maxStreams >= newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive)
                return connection

            case .leased(let connection, let usedStreams, let maxStreams, let keepAlive):
                precondition(maxStreams >= usedStreams + newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: usedStreams + newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive)
                return connection

            case .backingOff, .starting, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func release(streams returnedStreams: Int) -> ConnectionAvailableInfo {
            switch self.state {
            case .leased(let connection, let usedStreams, let maxStreams, let keepAlive):
                precondition(usedStreams >= returnedStreams)
                let newUsedStreams = usedStreams - returnedStreams
                let availableStreams = maxStreams - (newUsedStreams + keepAlive.usedStreams)
                if newUsedStreams == 0 {
                    self.state = .idle(connection, maxStreams: maxStreams, keepAlive: keepAlive)
                    return .idle(availableStreams: availableStreams, newIdle: true)
                } else {
                    self.state = .leased(connection, usedStreams: newUsedStreams, maxStreams: maxStreams, keepAlive: keepAlive)
                    return .leased(availableStreams: availableStreams)
                }
            case .backingOff, .starting, .idle, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func runKeepAliveIfIdle(reducesAvailableStreams: Bool) -> Connection? {
            switch self.state {
            case .idle(let connection, let maxStreams, .notRunning):
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .running(reducesAvailableStreams))
                return connection
            case .leased, .closed, .closing, .idle(_, _, .running):
                return nil
            case .backingOff, .starting:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func keepAliveSucceeded() -> ConnectionAvailableInfo? {
            switch self.state {
            case .idle(let connection, let maxStreams, .running):
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .notRunning)
                return .idle(availableStreams: maxStreams, newIdle: false)
            case .leased(let connection, let usedStreams, let maxStreams, .running):
                self.state = .leased(connection, usedStreams: usedStreams, maxStreams: maxStreams, keepAlive: .notRunning)
                return .leased(availableStreams: maxStreams - usedStreams)
            case .closed, .closing:
                return nil
            case .backingOff, .starting, .leased(_, _, _, .notRunning), .idle(_, _, .notRunning):
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func close() -> Connection {
            switch self.state {
            case .idle(let connection, maxStreams: _, keepAlive: _):
                self.state = .closing(connection)
                return connection
            case .backingOff, .starting, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func closeIfIdle() -> (Connection, Bool)? {
            switch self.state {
            case .idle(let connection, maxStreams: _, let keepAlive):
                self.state = .closing(connection)
                return (connection, keepAlive == .notRunning)
            case .leased, .closed:
                return nil
            case .backingOff, .starting, .closing:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        enum ShutdownAction {
            case none
            case close(Connection, wasIdle: Bool)
            case cancelBackoff(ConnectionID)
        }

        mutating func shutdown() -> ShutdownAction {
            switch self.state {
            case .starting, .closing:
                return .none
            case .backingOff:
                return .cancelBackoff(self.id)
            case .idle(let connection, maxStreams: _, keepAlive: _):
                self.state = .closing(connection)
                return .close(connection, wasIdle: true)
            case .leased(let connection, usedStreams: _, maxStreams: _, keepAlive: _):
                self.state = .closing(connection)
                return .close(connection, wasIdle: false)
            case .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        enum StateBeforeClose {
            case idle
            case leased
            case runKeepAlive
            case closing
        }

        mutating func closed() -> StateBeforeClose {
            switch self.state {
            case .idle:
                self.state = .closed
                return .idle
            case .leased:
                self.state = .closed
                return .leased
            case .closing:
                self.state = .closed
                return .closing
            case .starting, .backingOff, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }
    }

    enum ConnectionAvailableInfo: Equatable {
        case leased(availableStreams: Int)
        case idle(availableStreams: Int, newIdle: Bool)
    }

    struct EventLoopConnections {
        struct Stats: Hashable {
            var connecting: UInt16 = 0
            var backingOff: UInt16 = 0
            var idle: UInt16 = 0
            var leased: UInt16 = 0
            var runningKeepAlive: UInt16 = 0
            var closing: UInt16 = 0

            var soonAvailable: UInt16 {
                self.connecting + self.backingOff + self.runningKeepAlive
            }

            var active: UInt16 {
                self.idle + self.leased + self.runningKeepAlive + self.connecting + self.backingOff
            }
        }

        let eventLoop: any EventLoop

        /// The minimum number of connections
        let minimumConcurrentConnections: Int

        /// The maximum number of preserved connections
        let maximumConcurrentConnectionSoftLimit: Int

        /// The absolute maximum number of connections
        let maximumConcurrentConnectionHardLimit: Int

        let keepAliveReducesAvailableStreams: Bool

        /// A connectionID generator.
        private let generator: ConnectionIDGenerator

        /// The connections states
        private var connections: [ConnectionState]

        private(set) var stats = Stats()

        init(
            eventLoop: any EventLoop,
            generator: ConnectionIDGenerator,
            minimumConcurrentConnections: Int,
            maximumConcurrentConnectionSoftLimit: Int,
            maximumConcurrentConnectionHardLimit: Int,
            keepAliveReducesAvailableStreams: Bool
        ) {
            self.eventLoop = eventLoop
            self.generator = generator
            self.connections = []
            self.minimumConcurrentConnections = minimumConcurrentConnections
            self.maximumConcurrentConnectionSoftLimit = maximumConcurrentConnectionSoftLimit
            self.maximumConcurrentConnectionHardLimit = maximumConcurrentConnectionHardLimit
            self.keepAliveReducesAvailableStreams = keepAliveReducesAvailableStreams
        }

        var isEmpty: Bool {
            self.connections.isEmpty
        }

        var canGrow: Bool {
            self.stats.active < self.maximumConcurrentConnectionHardLimit
        }

        var soonAvailable: UInt16 {
            self.stats.soonAvailable
        }

        // MARK: - Mutations -

        /// A connection's use. Is it persisted or an overflow connection?
        enum ConnectionUse {
            case persisted
            case demand
            case overflow
        }

        /// Information around an idle connection.
        struct AvailableConnectionContext {
            /// The `EventLoop` the connection runs on.
            var eventLoop: EventLoop
            /// The connection's use. Either general purpose or for requests with `EventLoop`
            /// requirements.
            var use: ConnectionUse

            var info: ConnectionAvailableInfo
        }

        /// Information around the failed/closed connection.
        struct FailedConnectionContext {
            /// Connections that are currently starting
            var connectionsStarting: Int
        }

        mutating func refillConnections(_ requests: inout [ConnectionRequest]) {
            let existingConnections = self.stats.active
            let missingConnection = self.minimumConcurrentConnections - Int(existingConnections)
            guard missingConnection > 0 else {
                return
            }

            for _ in 0..<missingConnection {
                requests.append(self.createNewConnection())
            }
        }

        // MARK: Connection creation

        mutating func createNewDemandConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.minimumConcurrentConnections <= self.stats.active)
            guard self.maximumConcurrentConnectionSoftLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        mutating func createNewOverflowConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.maximumConcurrentConnectionSoftLimit <= self.stats.active)
            guard self.maximumConcurrentConnectionHardLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        private mutating func createNewConnection() -> ConnectionRequest {
            precondition(self.canGrow)
            self.stats.connecting += 1
            let connectionID = self.generator.next()
            let connection = ConnectionState(id: connectionID)
            self.connections.append(connection)
            return ConnectionRequest(eventLoop: self.eventLoop, connectionID: connectionID)
        }

        /// A new ``PostgresConnection`` was established.
        ///
        /// This will put the connection into the idle state.
        ///
        /// - Parameter connection: The new established connection.
        /// - Returns: An index and an IdleConnectionContext to determine the next action for the now idle connection.
        ///            Call ``parkConnection(at:)``, ``leaseConnection(at:)`` or ``closeConnection(at:)``
        ///            with the supplied index after this.
        mutating func newConnectionEstablished(_ connection: Connection) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connection.id }) else {
                preconditionFailure("There is a new connection that we didn't request!")
            }
            self.stats.connecting -= 1
            self.stats.idle += 1
            let connectionInfo = self.connections[index].connected(connection, maxStreams: 1)
            // TODO: If this is an overflow connection, but we are currently also creating a
            //       persisted connection, we might want to swap those.
            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        /// Move the ConnectionState to backingOff.
        ///
        /// - Parameter connectionID: The connectionID of the failed connection attempt
        /// - Returns: The eventLoop on which to schedule the backoff timer
        mutating func backoffNextConnectionAttempt(_ connectionID: Connection.ID) -> any EventLoop {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("We tried to create a new connection that we know nothing about?")
            }

            self.stats.connecting -= 1
            self.stats.backingOff += 1

            self.connections[index].failedToConnect()
            return self.eventLoop
        }

        enum BackoffDoneAction: Equatable {
            case createConnection(ConnectionRequest)
            case cancelIdleTimeoutTimer(ConnectionID)
            case none
        }

        mutating func backoffDone(_ connectionID: Connection.ID, retry: Bool) -> BackoffDoneAction {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("We tried to create a new connection that we know nothing about?")
            }

            self.stats.backingOff -= 1

            if retry || self.stats.active < self.minimumConcurrentConnections {
                self.stats.connecting += 1
                self.connections[index].retryConnect()
                return .createConnection(.init(eventLoop: self.eventLoop, connectionID: connectionID))
            }

            if let connectionID = self.swapForDeletion(index: index) {
                return .cancelIdleTimeoutTimer(connectionID)
            }

            return .none
        }

        private mutating func swapForDeletion(index indexToDelete: Int) -> ConnectionID? {
            let lastConnectedIndex = self.connections.lastIndex(where: { $0.isConnected })

            if lastConnectedIndex == nil || lastConnectedIndex! < indexToDelete {
                self.removeO1(indexToDelete)
                return nil
            }

            guard let lastConnectedIndex = lastConnectedIndex else { preconditionFailure() }

            switch indexToDelete {
            case 0..<self.minimumConcurrentConnections:
                // the connection to be removed is a persisted connection
                self.connections.swapAt(indexToDelete, lastConnectedIndex)
                self.removeO1(lastConnectedIndex)

                switch lastConnectedIndex {
                case 0..<self.minimumConcurrentConnections:
                    // a persisted connection was moved within the persisted connections. thats fine.
                    return nil

                case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                    // a demand connection was moved to a persisted connection. If it currently idle
                    // or ping ponging, we must cancel its idle timeout timer
                    if self.connections[indexToDelete].isIdleOrRunningKeepAlive {
                        return self.connections[indexToDelete].id
                    }
                    return nil

                case self.maximumConcurrentConnectionSoftLimit..<self.maximumConcurrentConnectionHardLimit:
                    // an overflow connection was moved to a demand connection. It has to be currently leased
                    precondition(self.connections[indexToDelete].isLeased)
                    return nil

                default:
                    return nil
                }

            case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                // the connection to be removed is a demand connection
                switch lastConnectedIndex {
                case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                    // an overflow connection was moved to a demand connection. It has to be currently leased
                    precondition(self.connections[indexToDelete].isLeased)
                    return nil

                default:
                    return nil
                }

            default:
                return nil
            }
        }

        private mutating func removeO1(_ indexToDelete: Int) {
            let lastIndex = self.connections.endIndex - 1

            if indexToDelete == lastIndex {
                self.connections.remove(at: indexToDelete)
            } else {
                self.connections.swapAt(indexToDelete, lastIndex)
                self.connections.remove(at: lastIndex)
            }
        }

        // MARK: Leasing and releasing

        /// Lease a connection, if an idle connection is available.
        ///
        /// - Returns: A connection to execute a request on.
        mutating func leaseConnection() -> (Connection, ConnectionUse)? {
            if self.stats.idle == 0 {
                return nil
            }

            guard let index = self.findIdleConnection() else {
                preconditionFailure("Stats and actual count are of.")
            }

            let use = self.getConnectionUse(index: index)

            self.stats.idle -= 1
            self.stats.leased += 1
            return (self.connections[index].lease(), use)
        }

        enum LeasedConnectionOrStartingCount {
            case leasedConnection(Connection, ConnectionUse)
            case startingCount(UInt16)
        }

        mutating func leaseConnectionOrSoonAvailableConnectionCount() -> LeasedConnectionOrStartingCount {
            if let (connection, use) = self.leaseConnection() {
                return .leasedConnection(connection, use)
            }
            return .startingCount(self.stats.soonAvailable)
        }

        mutating func leaseConnection(at index: Int) -> Connection {
            self.stats.idle -= 1
            self.stats.leased += 1
            return self.connections[index].lease()
        }

        func parkConnection(at index: Int) -> Connection.ID {
            precondition(self.connections[index].isIdle)
            return self.connections[index].id
        }

        /// A connection was released.
        ///
        /// This will put the position into the idle state.
        ///
        /// - Parameter connectionID: The released connection's id.
        /// - Returns: An index and an IdleConnectionContext to determine the next action for the now idle connection.
        ///            Call ``leaseConnection(at:)`` or ``closeConnection(at:)`` with the supplied index after
        ///            this. If you want to park the connection no further call is required.
        mutating func releaseConnection(_ connectionID: Connection.ID, streams: Int) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle += 1
            self.stats.leased -= 1

            let connectionInfo = self.connections[index].release(streams: streams)
            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        mutating func keepAliveIfIdle(_ connectionID: Connection.ID) -> Connection? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of ping pong)
                // was already removed from the state machine.
                return nil
            }

            guard let connection = self.connections[index].runKeepAliveIfIdle(reducesAvailableStreams: self.keepAliveReducesAvailableStreams) else {
                return nil
            }

            self.stats.idle -= 1
            self.stats.runningKeepAlive += 1

            return connection
        }

        mutating func keepAliveSucceeded(_ connectionID: Connection.ID) -> (Int, AvailableConnectionContext)? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle += 1
            self.stats.runningKeepAlive -= 1

            guard let connectionInfo = self.connections[index].keepAliveSucceeded() else {
                return nil
            }
            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        // MARK: Connection close/removal

        /// Closes the connection at the given index.
        mutating func closeConnection(at index: Int) -> Connection {
            self.stats.idle -= 1
            self.stats.closing += 1
            return self.connections[index].close()
        }

        mutating func closeConnectionIfIdle(_ connectionID: Connection.ID) -> (Connection, Bool)? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of timeout)
                // was already removed from the state machine.
                return nil
            }

            if index < self.minimumConcurrentConnections {
                // because of a race a connection might receive a idle timeout after it was moved into
                // the persisted connections. If a connection is now persisted, we now need to ignore
                // the trigger
                return nil
            }

            guard let (connection, cancelKeepAliveTimer) = self.connections[index].closeIfIdle() else {
                return nil
            }

            if cancelKeepAliveTimer {
                self.stats.idle -= 1
            } else {
                self.stats.runningKeepAlive -= 1
            }
            self.stats.closing += 1

            return (connection, cancelKeepAliveTimer)
        }

        // MARK: Connection failure

        /// Fail a connection. Call this method, if a connection is closed, did not startup correctly, or the backoff time is done.
        ///
        /// This will put the position into the closed state.
        ///
        /// - Parameter connectionID: The failed connection's id.
        /// - Returns: An optional index and an IdleConnectionContext to determine the next action for the closed connection.
        ///            You must call ``removeConnection(at:)`` or ``replaceConnection(at:)`` with the
        ///            supplied index after this. If nil is returned the connection was closed by the state machine and was
        ///            therefore already removed.
        mutating func failConnection(_ connectionID: Connection.ID) -> FailedConnectionContext? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                return nil
            }

            switch self.connections[index].closed() {
            case .idle:
                self.stats.idle -= 1
            case .closing:
                self.stats.closing -= 1
            case .runKeepAlive:
                self.stats.runningKeepAlive -= 1
            case .leased:
                self.stats.leased -= 1
            }
            let lastIndex = self.connections.endIndex - 1

            if index == lastIndex {
                self.connections.remove(at: index)
            } else {
                self.connections.swapAt(index, lastIndex)
                self.connections.remove(at: lastIndex)
            }

            return FailedConnectionContext(connectionsStarting: 0)
        }

        // MARK: Shutdown

        mutating func shutdown(_ cleanup: inout ConnectionAction.Shutdown) {
            self.connections = self.connections.enumerated().compactMap { index, connectionState in
                var connectionState = connectionState
                switch connectionState.shutdown() {
                case .cancelBackoff(let connectionID):
                    cleanup.backoffTimersToCancel.append(connectionID)
                    return nil

                case .close(let connection, let wasIdle):
                    let cancelIdleTimer = index >= self.minimumConcurrentConnections
                    cleanup.connections.append(
                        .init(cancelIdleTimer: cancelIdleTimer, cancelKeepAliveTimer: wasIdle, connection: connection)
                    )
                    return connectionState

                case .none:
                    return connectionState
                }
            }
        }

        // MARK: - Private functions -

        private func getConnectionUse(index: Int) -> ConnectionUse {
            switch index {
            case 0..<self.minimumConcurrentConnections:
                return .persisted
            case self.minimumConcurrentConnections..<self.maximumConcurrentConnectionSoftLimit:
                return .demand
            case self.maximumConcurrentConnectionSoftLimit...:
                return .overflow
            default:
                preconditionFailure()
            }
        }

        private func makeAvailableConnectionContextForConnection(at index: Int, info: ConnectionAvailableInfo) -> AvailableConnectionContext {
            precondition(self.connections[index].isAvailable)
            let use = self.getConnectionUse(index: index)
            return AvailableConnectionContext(eventLoop: self.eventLoop, use: use, info: info)
        }

        private func findIdleConnection() -> Int? {
            return self.connections.firstIndex(where: { $0.isAvailable })
        }
    }
}
