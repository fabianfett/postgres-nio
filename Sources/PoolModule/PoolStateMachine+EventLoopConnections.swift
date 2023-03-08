import Atomics
import NIOCore

extension PoolStateMachine {

    @usableFromInline
    /*private*/ struct ConnectionState {
        @usableFromInline
        enum State {
            @usableFromInline
            enum KeepAlive: Equatable {
                case notRunning
                case running(Bool)

                @usableFromInline
                var usedStreams: UInt16 {
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
            case idle(Connection, maxStreams: UInt16, keepAlive: KeepAlive)
            /// The connection is leased and executing a query. Valid transitions to: `.idle` and `.closed`
            case leased(Connection, usedStreams: UInt16, maxStreams: UInt16, keepAlive: KeepAlive)
            /// The connection is closing. Valid transitions to: `.closed`
            case closing(Connection)
            /// The connection is closed. Final state.
            case closed
        }

        @usableFromInline
        let id: Connection.ID

        @usableFromInline
        private(set) var state: State

        @inlinable
        init(id: Connection.ID) {
            self.id = id
            self.state = .starting
        }

        @inlinable
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

        @inlinable
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

        @usableFromInline
        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .closed, .closing, .idle:
                return false
            }
        }

        @usableFromInline
        var isIdleOrRunningKeepAlive: Bool {
            switch self.state {
            case .idle:
                return true
            case .backingOff, .starting, .closed, .closing, .leased:
                return false
            }
        }

        @usableFromInline
        var isConnected: Bool {
            switch self.state {
            case .idle, .leased:
                return true
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        @inlinable
        mutating func connected(_ connection: Connection, maxStreams: UInt16) -> ConnectionAvailableInfo {
            switch self.state {
            case .starting:
                self.state = .idle(connection, maxStreams: maxStreams, keepAlive: .notRunning)
                return .idle(availableStreams: maxStreams, newIdle: true)
            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        /// The connection failed to start
        @inlinable
        mutating func failedToConnect() {
            switch self.state {
            case .starting:
                self.state = .backingOff
            case .backingOff, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @usableFromInline
        mutating func retryConnect() {
            switch self.state {
            case .backingOff:
                self.state = .starting
            case .starting, .idle, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func lease(streams newLeasedStreams: UInt16 = 1) -> (Connection, wasIdle: Bool) {
            switch self.state {
            case .idle(let connection, let maxStreams, let keepAlive):
                precondition(maxStreams >= newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive)
                return (connection, wasIdle: true)

            case .leased(let connection, let usedStreams, let maxStreams, let keepAlive):
                precondition(maxStreams >= usedStreams + newLeasedStreams + keepAlive.usedStreams, "Invalid state: \(self.state)")
                self.state = .leased(connection, usedStreams: usedStreams + newLeasedStreams, maxStreams: maxStreams, keepAlive: keepAlive)
                return (connection, wasIdle: false)

            case .backingOff, .starting, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @inlinable
        mutating func release(streams returnedStreams: UInt16) -> ConnectionAvailableInfo {
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

        @inlinable
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

        @inlinable
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

        @inlinable
        mutating func close() -> Connection {
            switch self.state {
            case .idle(let connection, maxStreams: _, keepAlive: _):
                self.state = .closing(connection)
                return connection
            case .backingOff, .starting, .leased, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        @usableFromInline
        struct IdleState {
            @usableFromInline var runningKeepAlive: Bool
            @usableFromInline var maxStreams: UInt16

            @usableFromInline
            init(runningKeepAlive: Bool, maxStreams: UInt16) {
                self.runningKeepAlive = runningKeepAlive
                self.maxStreams = maxStreams
            }
        }

        @inlinable
        mutating func closeIfIdle() -> (Connection, IdleState)? {
            switch self.state {
            case .idle(let connection, let maxStreams, let keepAlive):
                self.state = .closing(connection)
                return (connection, .init(runningKeepAlive: keepAlive != .notRunning, maxStreams: maxStreams))
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
            case idle(maxStreams: UInt16, runningKeepAlive: Bool)
            case leased(leasedStreams: UInt16, maxStreams: UInt16, runningKeepAlive: Bool)
            case closing
        }

        mutating func closed() -> StateBeforeClose {
            switch self.state {
            case .idle(_, let maxStreams, .running):
                self.state = .closed
                return .idle(maxStreams: maxStreams, runningKeepAlive: true)
            case .idle(_, let maxStreams, .notRunning):
                self.state = .closed
                return .idle(maxStreams: maxStreams, runningKeepAlive: false)
            case .leased(_, let leasedStreams, let maxStreams, .running):
                self.state = .closed
                return .leased(leasedStreams: leasedStreams, maxStreams: maxStreams, runningKeepAlive: true)
            case .leased(_, let leasedStreams, let maxStreams, .notRunning):
                self.state = .closed
                return .leased(leasedStreams: leasedStreams, maxStreams: maxStreams, runningKeepAlive: false)
            case .closing:
                self.state = .closed
                return .closing
            case .starting, .backingOff, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }
    }

    @usableFromInline
    enum ConnectionAvailableInfo: Equatable {
        case leased(availableStreams: UInt16)
        case idle(availableStreams: UInt16, newIdle: Bool)

        @usableFromInline
        var availableStreams: UInt16 {
            switch self {
            case .leased(let availableStreams):
                return availableStreams
            case .idle(let availableStreams, newIdle: _):
                return availableStreams
            }
        }
    }

    @usableFromInline
    struct ConnectionLeasedInfo: Equatable {
        @usableFromInline
        var use: EventLoopConnections.ConnectionUse
        @usableFromInline
        var wasIdle: Bool

        @usableFromInline
        init(use: EventLoopConnections.ConnectionUse, wasIdle: Bool) {
            self.use = use
            self.wasIdle = wasIdle
        }
    }

    @usableFromInline
    struct EventLoopConnections {
        @usableFromInline
        struct Stats: Hashable {
            @usableFromInline var connecting: UInt16 = 0
            @usableFromInline var backingOff: UInt16 = 0
            @usableFromInline var idle: UInt16 = 0
            @usableFromInline var leased: UInt16 = 0
            @usableFromInline var runningKeepAlive: UInt16 = 0
            @usableFromInline var closing: UInt16 = 0

            @usableFromInline var availableStreams: UInt16 = 0
            @usableFromInline var leasedStreams: UInt16 = 0

            @usableFromInline var soonAvailable: UInt16 {
                self.connecting + self.backingOff + self.runningKeepAlive
            }

            @usableFromInline var active: UInt16 {
                self.idle + self.leased + self.connecting + self.backingOff
            }
        }

        @usableFromInline
        let eventLoop: any EventLoop

        /// The minimum number of connections
        @usableFromInline
        let minimumConcurrentConnections: Int

        /// The maximum number of preserved connections
        @usableFromInline
        let maximumConcurrentConnectionSoftLimit: Int

        /// The absolute maximum number of connections
        @usableFromInline
        let maximumConcurrentConnectionHardLimit: Int

        @usableFromInline
        let keepAliveReducesAvailableStreams: Bool

        /// A connectionID generator.
        @usableFromInline
        let generator: ConnectionIDGenerator

        /// The connections states
        @usableFromInline
        private(set) var connections: [ConnectionState]

        @usableFromInline
        private(set) var stats = Stats()

        @inlinable
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

        @usableFromInline
        var canGrow: Bool {
            self.stats.active < self.maximumConcurrentConnectionHardLimit
        }

        @usableFromInline
        var soonAvailable: UInt16 {
            self.stats.soonAvailable
        }

        // MARK: - Mutations -

        /// A connection's use. Is it persisted or an overflow connection?
        @usableFromInline
        enum ConnectionUse {
            case persisted
            case demand
            case overflow
        }

        /// Information around an idle connection.
        @usableFromInline
        struct AvailableConnectionContext {
            /// The `EventLoop` the connection runs on.
            @usableFromInline
            var eventLoop: EventLoop
            /// The connection's use. Either general purpose or for requests with `EventLoop`
            /// requirements.
            @usableFromInline
            var use: ConnectionUse

            @usableFromInline
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

        @inlinable
        mutating func createNewDemandConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.minimumConcurrentConnections <= self.stats.active)
            guard self.maximumConcurrentConnectionSoftLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        @inlinable
        mutating func createNewOverflowConnectionIfPossible() -> ConnectionRequest? {
            precondition(self.maximumConcurrentConnectionSoftLimit <= self.stats.active)
            guard self.maximumConcurrentConnectionHardLimit > self.stats.active else {
                return nil
            }
            return self.createNewConnection()
        }

        @inlinable
        /*private*/ mutating func createNewConnection() -> ConnectionRequest {
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
        @inlinable
        mutating func newConnectionEstablished(_ connection: Connection, maxStreams: UInt16) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connection.id }) else {
                preconditionFailure("There is a new connection that we didn't request!")
            }
            self.stats.connecting -= 1
            self.stats.idle += 1
            self.stats.availableStreams += maxStreams
            let connectionInfo = self.connections[index].connected(connection, maxStreams: maxStreams)
            // TODO: If this is an overflow connection, but we are currently also creating a
            //       persisted connection, we might want to swap those.
            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        /// Move the ConnectionState to backingOff.
        ///
        /// - Parameter connectionID: The connectionID of the failed connection attempt
        /// - Returns: The eventLoop on which to schedule the backoff timer
        @inlinable
        mutating func backoffNextConnectionAttempt(_ connectionID: Connection.ID) -> any EventLoop {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("We tried to create a new connection that we know nothing about?")
            }

            self.stats.connecting -= 1
            self.stats.backingOff += 1

            self.connections[index].failedToConnect()
            return self.eventLoop
        }

        @usableFromInline
        enum BackoffDoneAction: Equatable {
            case createConnection(ConnectionRequest)
            case cancelIdleTimeoutTimer(ConnectionID)
            case none
        }

        @inlinable
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

        @inlinable
        /*private*/ mutating func swapForDeletion(index indexToDelete: Int) -> ConnectionID? {
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

        @inlinable
        /*private*/ mutating func removeO1(_ indexToDelete: Int) {
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
        @inlinable
        mutating func leaseConnection() -> (Connection, ConnectionLeasedInfo)? {
            if self.stats.availableStreams == 0 {
                return nil
            }

            guard let index = self.findAvailableConnection() else {
                preconditionFailure("Stats and actual count are of.")
            }

            let use = self.getConnectionUse(index: index)
            let (connection, wasIdle) = self.leaseConnection(at: index, streams: 1)
            return (connection, .init(use: use, wasIdle: wasIdle))
        }

        @usableFromInline
        enum LeasedConnectionOrStartingCount {
            case leasedConnection(Connection, ConnectionLeasedInfo)
            case startingCount(UInt16)
        }

        @inlinable
        mutating func leaseConnectionOrSoonAvailableConnectionCount() -> LeasedConnectionOrStartingCount {
            if let (connection, info) = self.leaseConnection() {
                return .leasedConnection(connection, info)
            }
            return .startingCount(self.stats.soonAvailable)
        }

        @inlinable
        mutating func leaseConnection(at index: Int, streams: UInt16) -> (Connection, wasIdle: Bool) {
            let (connection, wasIdle) = self.connections[index].lease(streams: streams)

            if wasIdle {
                self.stats.idle -= 1
                self.stats.leased += 1
            }
            self.stats.leasedStreams += streams
            self.stats.availableStreams -= streams
            return (connection, wasIdle)
        }

        @inlinable
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
        @inlinable
        mutating func releaseConnection(_ connectionID: Connection.ID, streams: UInt16) -> (Int, AvailableConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            let connectionInfo = self.connections[index].release(streams: streams)
            self.stats.availableStreams += streams
            self.stats.leasedStreams -= streams
            switch connectionInfo {
            case .idle:
                self.stats.idle += 1
                self.stats.leased -= 1
            case .leased:
                break
            }

            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        @inlinable
        mutating func keepAliveIfIdle(_ connectionID: Connection.ID) -> Connection? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of ping pong)
                // was already removed from the state machine.
                return nil
            }

            guard let connection = self.connections[index].runKeepAliveIfIdle(reducesAvailableStreams: self.keepAliveReducesAvailableStreams) else {
                return nil
            }

            self.stats.runningKeepAlive += 1
            if self.keepAliveReducesAvailableStreams {
                self.stats.availableStreams -= 1
            }

            return connection
        }

        @inlinable
        mutating func keepAliveSucceeded(_ connectionID: Connection.ID) -> (Int, AvailableConnectionContext)? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            guard let connectionInfo = self.connections[index].keepAliveSucceeded() else {
                // if we don't get connection info here this means, that the connection already was
                // transitioned to closing. when we did this we already decremented the
                // runningKeepAlive timer.
                return nil
            }

            self.stats.runningKeepAlive -= 1
            if self.keepAliveReducesAvailableStreams {
                self.stats.availableStreams += 1
            }

            let context = self.makeAvailableConnectionContextForConnection(at: index, info: connectionInfo)
            return (index, context)
        }

        // MARK: Connection close/removal

        /// Closes the connection at the given index.
        @inlinable
        mutating func closeConnection(at index: Int) -> Connection {
            self.stats.idle -= 1
            self.stats.closing += 1
            return self.connections[index].close()
        }

        @inlinable
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

            guard let (connection, idleState) = self.connections[index].closeIfIdle() else {
                return nil
            }

            self.stats.idle -= 1
            self.stats.closing += 1

            if idleState.runningKeepAlive {
                self.stats.runningKeepAlive -= 1
                if self.keepAliveReducesAvailableStreams {
                    self.stats.availableStreams += 1
                }
            }

            self.stats.availableStreams -= idleState.maxStreams

            return (connection, !idleState.runningKeepAlive)
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
            case .idle(let maxStreams, let runningKeepAlive):
                self.stats.idle -= 1
                self.stats.availableStreams -= maxStreams
                if runningKeepAlive {
                    self.stats.runningKeepAlive -= 1
                }
            case .closing:
                self.stats.closing -= 1
            case .leased(let usedStreams, let maxStreams, let runningKeepAlive):
                self.stats.leased -= 1
                self.stats.availableStreams -= maxStreams - usedStreams
                self.stats.leasedStreams -= usedStreams
                if runningKeepAlive {
                    self.stats.runningKeepAlive -= 1
                }
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

        @usableFromInline
        /*private*/ func getConnectionUse(index: Int) -> ConnectionUse {
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

        @usableFromInline
        /*private*/ func makeAvailableConnectionContextForConnection(at index: Int, info: ConnectionAvailableInfo) -> AvailableConnectionContext {
            precondition(self.connections[index].isAvailable)
            let use = self.getConnectionUse(index: index)
            return AvailableConnectionContext(eventLoop: self.eventLoop, use: use, info: info)
        }

        @inlinable
        /*private*/ func findAvailableConnection() -> Int? {
            return self.connections.firstIndex(where: { $0.isAvailable })
        }
    }
}
