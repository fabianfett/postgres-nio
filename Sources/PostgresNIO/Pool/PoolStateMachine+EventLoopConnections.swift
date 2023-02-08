#if swift(>=5.7)
import NIOCore

@available(macOS 13.0, iOS 16.0, *)
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

        var isConnecting: Bool {
            switch self.state {
            case .starting:
                return true
            case .backingOff, .closing, .closed, .idle, .leased, .pingpong:
                return false
            }
        }

        var isBackingOff: Bool {
            switch self.state {
            case .backingOff:
                return true
            case .starting, .closing, .closed, .idle, .leased, .pingpong:
                return false
            }
        }

        var isIdle: Bool {
            switch self.state {
            case .idle:
                return true
            case .backingOff, .starting, .leased, .closing, .closed, .pingpong:
                return false
            }
        }

        var canOrWillBeAbleToExecuteRequests: Bool {
            switch self.state {
            case .leased, .backingOff, .idle, .starting, .pingpong:
                return true
            case .closing, .closed:
                return false
            }
        }

        var isLeased: Bool {
            switch self.state {
            case .leased:
                return true
            case .backingOff, .starting, .idle, .closed, .closing, .pingpong:
                return false
            }
        }

        var isActive: Bool {
            switch self.state {
            case .leased, .idle, .pingpong:
                return true
            case .backingOff, .starting, .closed, .closing:
                return false
            }
        }

        var isClosed: Bool {
            switch self.state {
            case .closed:
                return true
            case .backingOff, .starting, .closing, .leased, .idle, .pingpong:
                return false
            }
        }

        var idleSince: NIODeadline? {
            switch self.state {
            case .idle(_, since: let idleSince), .pingpong(_, since: let idleSince):
                return idleSince
            case .backingOff, .starting, .leased, .closed, .closing:
                return nil
            }
        }

        init(id: Connection.ID) {
            self.id = id
            self.state = .starting
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

        mutating func pingpong() -> Connection {
            switch self.state {
            case .idle(let connection, let since):
                self.state = .pingpong(connection, since: since)
                return connection
            case .backingOff, .starting, .leased, .pingpong, .closing, .closed:
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

        mutating func fail() {
            switch self.state {
            case .starting, .backingOff, .idle, .leased, .pingpong, .closing:
                self.state = .closed
            case .closed:
                preconditionFailure("Invalid state: \(self.state)")
            }
        }
    }

    struct EventLoopConnections {
        struct Stats {
            var idle: UInt16 = 0
            var leased: UInt16 = 0
            var pingpong: UInt16 = 0
            var connecting: UInt16 = 0
            var backingOff: UInt16 = 0

            var connectingOrBackingOff: UInt16 {
                self.connecting + self.backingOff
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

        private var stats = Stats()

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
            self.connections.count < self.maximumConcurrentConnectionHardLimit
        }

        /// Is there at least one connection that is able to run requests
        var hasActiveConnections: Bool {
            self.connections.contains(where: { $0.isActive })
        }

        // MARK: - Mutations -

        /// A connection's use. Is it persisted or an overflow connection?
        enum ConnectionUse {
            case persisted
            case overflow
            case oneof
        }

        /// Information around an idle connection.
        struct IdleConnectionContext {
            /// The `EventLoop` the connection runs on.
            var eventLoop: EventLoop
            /// The connection's use. Either general purpose or for requests with `EventLoop`
            /// requirements.
            var use: ConnectionUse
        }

        /// Information around the failed/closed connection.
        struct FailedConnectionContext {
            /// Connections that are currently starting
            var connectionsStarting: Int
        }

        mutating func refillConnections(_ requests: inout [ConnectionRequest]) {
            var existingConnections = 0
            for connection in self.connections {
                if connection.canOrWillBeAbleToExecuteRequests {
                    existingConnections += 0
                }
            }

            let missingConnection = self.minimumConcurrentConnections - existingConnections
            guard missingConnection > 0 else {
                return
            }

            for _ in 0..<missingConnection {
                requests.append(self.createNewConnection())
            }
        }

        // MARK: Connection creation

        mutating func createNewConnection() -> ConnectionRequest {
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
            let context = self.generateIdleConnectionContextForConnection(at: index)
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

        // MARK: Leasing and releasing

        /// Lease a connection, if an idle connection is available.
        ///
        /// - Returns: A connection to execute a request on.
        mutating func leaseConnection() -> Connection? {
            guard let index = self.findIdleConnection() else {
                return nil
            }

            return self.connections[index].lease()
        }

        enum LeasedConnectionOrStartingCount {
            case leasedConnection(Connection)
            case startingCount(UInt16)
        }

        mutating func leaseConnectionOrReturnStartingCount() -> LeasedConnectionOrStartingCount {
            if let index = self.findIdleConnection() {
                return .leasedConnection(self.connections[index].lease())
            }
            return .startingCount(self.stats.connectingOrBackingOff)
        }

        mutating func leaseConnection(at index: Int) -> Connection {
            self.connections[index].lease()
        }

        func parkConnection(at index: Int) -> Connection.ID {
            precondition(self.connections[index].isIdle)
            return self.connections[index].id
        }

        /// A new HTTP/1.1 connection was released.
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

            self.connections[index].release()
            let context = self.generateIdleConnectionContextForConnection(at: index)
            return (index, context)
        }

        mutating func pingpong(_ connectionID: Connection.ID) -> Connection {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle -= 1
            self.stats.pingpong += 1

            return self.connections[index].pingpong()
        }

        mutating func pingpongDone(_ connectionID: Connection.ID) -> (Int, IdleConnectionContext) {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                preconditionFailure("A connection that we don't know was released? Something is very wrong...")
            }

            self.stats.idle += 1
            self.stats.pingpong -= 1

            self.connections[index].release()
            let context = self.generateIdleConnectionContextForConnection(at: index)
            return (index, context)
        }

        // MARK: Connection close/removal

        /// Closes the connection at the given index.
        mutating func closeConnection(at index: Int) -> Connection {
            return self.connections[index].close()
        }

        mutating func closeConnectionIfIdle(_ connectionID: Connection.ID) -> Connection? {
            guard let index = self.connections.firstIndex(where: { $0.id == connectionID }) else {
                // because of a race this connection (connection close runs against trigger of timeout)
                // was already removed from the state machine.
                return nil
            }

            guard self.connections[index].isIdle else {
                // connection is not idle anymore, we may have just leased it for a request
                return nil
            }

            return self.closeConnection(at: index)
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

            self.connections[index].fail()
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

//        mutating func shutdown() -> CleanupContext {
//            var cleanupContext = CleanupContext()
//            let initialOverflowIndex = self.overflowIndex
//
//            self.connections = self.connections.enumerated().compactMap { index, connectionState in
//                switch connectionState.cleanup(&cleanupContext) {
//                case .removeConnection:
//                    // If the connection has an index smaller than the previous overflow index,
//                    // we deal with a general purpose connection.
//                    // For this reason we need to decrement the overflow index.
//                    if index < initialOverflowIndex {
//                        self.overflowIndex = self.connections.index(before: self.overflowIndex)
//                    }
//                    return nil
//
//                case .keepConnection:
//                    return connectionState
//                }
//            }
//
//            return cleanupContext
//        }

        // MARK: - Private functions -

        private func generateIdleConnectionContextForConnection(at index: Int) -> IdleConnectionContext {
            precondition(self.connections[index].isIdle)
            let use: ConnectionUse
            if index < self.minimumConcurrentConnections {
                use = .persisted
            } else if index < self.maximumConcurrentConnectionSoftLimit {
                use = .overflow
            } else {
                use = .oneof
            }
            return IdleConnectionContext(eventLoop: self.eventLoop, use: use)
        }

        private func findIdleConnection() -> Int? {
            return self.connections.firstIndex(where: { $0.isIdle })
        }


    }

}
#endif

import Atomics

extension PostgresConnection.ID {
    static let globalGenerator = Generator()

    struct Generator {
        private let atomic: ManagedAtomic<Int>

        init() {
            self.atomic = .init(0)
        }

        func next() -> Int {
            return self.atomic.loadThenWrappingIncrement(ordering: .relaxed)
        }
    }
}
