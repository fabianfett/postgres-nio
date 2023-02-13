import Atomics
import NIOCore

extension PoolStateMachine {

    private struct ConnectionState {
        enum State {
            /// The pool is creating a connection. Valid transitions are to: `.backingOff`, `.idle`, and `.closed`
            case starting
            /// The pool is waiting to retry establishing a connection. Valid transitions are to: `.closed`.
            /// This means, the connection can be removed from the connections without cancelling external
            /// state. The connection state can then be replaced by a new one.
            case backingOff
            /// The connection is `idle` and ready to execute a new query. Valid transitions to: `.pingpong`, `.leased`,
            /// `.closing` and `.closed`
            case idle(Connection, since: NIODeadline)
            /// The connection is ping-ponging. Valid transitions to: `.idle` and `.closed`
            case pingpong(Connection, since: NIODeadline)
            /// The connection is leased and executing a query. Valid transitions to: `.idle` and `.closed`
            case leased(Connection)
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
            case .idle:
                return true
            case .backingOff, .starting, .closed, .closing, .leased, .pingpong:
                return false
            }
        }

        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .closed, .closing, .idle, .pingpong:
                return false
            }
        }

        var isIdleOrPingPonging: Bool {
            switch self.state {
            case .idle, .pingpong:
                return true
            case .backingOff, .starting, .closed, .closing, .leased:
                return false
            }
        }

        var isConnected: Bool {
            switch self.state {
            case .idle, .pingpong, .leased:
                return true
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        mutating func connected(_ connection: Connection) {
            switch self.state {
            case .starting:
                self.state = .idle(connection, since: .now())
            case .backingOff, .idle, .leased, .pingpong, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        /// The connection failed to start
        mutating func failedToConnect() {
            switch self.state {
            case .starting:
                self.state = .backingOff
            case .backingOff, .idle, .leased, .pingpong, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func retryConnect() {
            switch self.state {
            case .backingOff:
                self.state = .starting
            case .starting, .idle, .leased, .pingpong, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func lease() -> Connection {
            switch self.state {
            case .idle(let connection, since: _):
                self.state = .leased(connection)
                return connection
            case .backingOff, .starting, .leased, .pingpong, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func release() {
            switch self.state {
            case .leased(let connection):
                self.state = .idle(connection, since: .now())
            case .pingpong(let connection, let since):
                self.state = .idle(connection, since: since)
            case .backingOff, .starting, .idle, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func pingPongIfIdle() -> Connection? {
            switch self.state {
            case .idle(let connection, let since):
                self.state = .pingpong(connection, since: since)
                return connection
            case .leased, .closed, .closing:
                return nil
            case .backingOff, .starting, .pingpong:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func close() -> Connection {
            switch self.state {
            case .idle(let connection, since: _):
                self.state = .closing(connection)
                return connection
            case .backingOff, .starting, .leased, .pingpong, .closing, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        mutating func closeIfIdle() -> (Connection, Bool)? {
            switch self.state {
            case .idle(let connection, since: _):
                self.state = .closing(connection)
                return (connection, true)
            case .pingpong(let connection, since: _):
                self.state = .closing(connection)
                return (connection, false)
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
            case .idle(let connection, since: _):
                self.state = .closing(connection)
                return .close(connection, wasIdle: true)
            case .leased(let connection), .pingpong(let connection, since: _):
                self.state = .closing(connection)
                return .close(connection, wasIdle: false)
            case .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }

        enum StateBeforeClose {
            case idle
            case leased
            case pingpong
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
            case .pingpong:
                self.state = .closed
                return .pingpong
            case .closing:
                self.state = .closed
                return .closing
            case .starting, .backingOff, .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }
    }

    struct EventLoopConnections {
        struct Stats: Hashable {
            var connecting: UInt16 = 0
            var backingOff: UInt16 = 0
            var idle: UInt16 = 0
            var leased: UInt16 = 0
            var pingpong: UInt16 = 0
            var closing: UInt16 = 0

            var soonAvailable: UInt16 {
                self.connecting + self.backingOff + self.pingpong
            }

            var active: UInt16 {
                self.idle + self.leased + self.pingpong + self.connecting + self.backingOff
            }
        }

        let eventLoop: any EventLoop

        /// The minimum number of connections
        let minimumConcurrentConnections: Int

        /// The maximum number of preserved connections
        let maximumConcurrentConnectionSoftLimit: Int

        /// The absolute maximum number of connections
        let maximumConcurrentConnectionHardLimit: Int

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
            maximumConcurrentConnectionHardLimit: Int
        ) {
            self.eventLoop = eventLoop
            self.generator = generator
            self.connections = []
            self.minimumConcurrentConnections = minimumConcurrentConnections
            self.maximumConcurrentConnectionSoftLimit = maximumConcurrentConnectionSoftLimit
            self.maximumConcurrentConnectionHardLimit = maximumConcurrentConnectionHardLimit
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
        struct IdleConnectionContext {
            /// The `EventLoop` the connection runs on.
            var eventLoop: EventLoop
            /// The connection's use. Either general purpose or for requests with `EventLoop`
            /// requirements.
            var use: ConnectionUse

            var hasBecomeIdle: Bool
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
        mutating func newConnectionEstablished(_ connection: Connection) -> (Int, IdleConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connection.id }) else {
                preconditionFailure("There is a new connection that we didn't request!")
            }
            self.stats.connecting -= 1
            self.stats.idle += 1
            self.connections[index].connected(connection)
            // TODO: If this is an overflow connection, but we are currently also creating a
            //       persisted connection, we might want to swap those.
            let context = self.generateIdleConnectionContextForConnection(at: index, hasBecomeIdle: true)
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
                    if self.connections[indexToDelete].isIdleOrPingPonging {
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
        mutating func leaseConnection() -> Connection? {
            if self.stats.idle == 0 {
                return nil
            }

            guard let index = self.findIdleConnection() else {
                preconditionFailure("Stats and actual count are of.")
            }

            self.stats.idle -= 1
            self.stats.leased += 1
            return self.connections[index].lease()
        }

        enum LeasedConnectionOrStartingCount {
            case leasedConnection(Connection)
            case startingCount(UInt16)
        }

        mutating func leaseConnectionOrSoonAvailableConnectionCount() -> LeasedConnectionOrStartingCount {
            if let connection = self.leaseConnection() {
                return .leasedConnection(connection)
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
        mutating func releaseConnection(_ connectionID: Connection.ID) -> (Int, IdleConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle += 1
            self.stats.leased -= 1

            self.connections[index].release()
            let context = self.generateIdleConnectionContextForConnection(at: index, hasBecomeIdle: true)
            return (index, context)
        }

        mutating func pingPongIfIdle(_ connectionID: Connection.ID) -> Connection? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of ping pong)
                // was already removed from the state machine.
                return nil
            }

            guard let connection = self.connections[index].pingPongIfIdle() else {
                return nil
            }

            self.stats.idle -= 1
            self.stats.pingpong += 1

            return connection
        }

        mutating func pingPongDone(_ connectionID: Connection.ID) -> (Int, IdleConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle += 1
            self.stats.pingpong -= 1

            self.connections[index].release()
            let context = self.generateIdleConnectionContextForConnection(at: index, hasBecomeIdle: false)
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

            guard let (connection, cancelPingPongTimer) = self.connections[index].closeIfIdle() else {
                return nil
            }

            if cancelPingPongTimer {
                self.stats.idle -= 1
            } else {
                self.stats.pingpong -= 1
            }
            self.stats.closing += 1

            return (connection, cancelPingPongTimer)
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
            case .pingpong:
                self.stats.pingpong -= 1
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
                        .init(cancelIdleTimer: cancelIdleTimer, cancelPingPongTimer: wasIdle, connection: connection)
                    )
                    return connectionState

                case .none:
                    return connectionState
                }
            }
        }

        // MARK: - Private functions -

        private func generateIdleConnectionContextForConnection(at index: Int, hasBecomeIdle: Bool) -> IdleConnectionContext {
            precondition(self.connections[index].isIdle)
            let use: ConnectionUse
            if index < self.minimumConcurrentConnections {
                use = .persisted
            } else if index < self.maximumConcurrentConnectionSoftLimit {
                use = .demand
            } else {
                use = .overflow
            }
            return IdleConnectionContext(eventLoop: self.eventLoop, use: use, hasBecomeIdle: hasBecomeIdle)
        }

        private func findIdleConnection() -> Int? {
            return self.connections.firstIndex(where: { $0.isIdle })
        }
    }
}
