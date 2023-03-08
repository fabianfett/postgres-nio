import NIOCore
import NIOConcurrencyHelpers

public protocol ConnectionKeepAliveBehavior: Sendable {
    associatedtype Connection: PooledConnection

    var keepAliveFrequency: TimeAmount? { get }

    func runKeepAlive(for connection: Connection) -> EventLoopFuture<Void>
}

public protocol ConnectionFactory {
    associatedtype Connection: PooledConnection
    associatedtype ConnectionID: Hashable where Connection.ID == ConnectionID
    associatedtype ConnectionIDGenerator: ConnectionIDGeneratorProtocol where ConnectionIDGenerator.ID == ConnectionID
    associatedtype Request: ConnectionRequestProtocol where Request.Connection == Connection
    associatedtype KeepAliveBehavior: ConnectionKeepAliveBehavior where KeepAliveBehavior.Connection == Connection
    associatedtype MetricsDelegate: ConnectionPoolMetricsDelegate where MetricsDelegate.ConnectionID == ConnectionID

    func makeConnection(
        on eventLoop: EventLoop,
        id: ConnectionID,
        for pool: ConnectionPool<Self, Connection, ConnectionID, ConnectionIDGenerator, Request, Request.ID, KeepAliveBehavior, MetricsDelegate>
    ) -> EventLoopFuture<ConnectionAndMetadata<Connection>>
}

public struct ConnectionAndMetadata<Connection: PooledConnection> {

    public var connection: Connection

    public var maximalStreamsOnConnection: UInt16

    public init(connection: Connection, maximalStreamsOnConnection: UInt16) {
        self.connection = connection
        self.maximalStreamsOnConnection = maximalStreamsOnConnection
    }
}

public struct ConnectionPoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the remote servers closes idle connections,
    /// the `ConnectionPool` will initiate new outbound connections proactively
    /// to avoid the number of available connections dropping below this number.
    public var minimumConnectionCount: Int

    /// The maximum number of connections to for this pool, to be preserved.
    public var maximumConnectionSoftLimit: Int

    public var maximumConnectionHardLimit: Int

    public var idleTimeout: TimeAmount

    public init() {
        self.minimumConnectionCount = System.coreCount
        self.maximumConnectionSoftLimit = System.coreCount
        self.maximumConnectionHardLimit = System.coreCount * 4
        self.idleTimeout = .seconds(60)
    }
}

public protocol PooledConnection: AnyObject, Sendable {
    associatedtype ID: Hashable

    var id: ID { get }

    var eventLoop: EventLoop { get }

    func onClose(_ closure: @escaping @Sendable () -> ())

    func close()
}

public protocol ConnectionIDGeneratorProtocol {
    associatedtype ID: Hashable

    func next() -> ID
}

public protocol ConnectionRequestProtocol {
    associatedtype ID: Hashable
    associatedtype Connection: PooledConnection

    var id: ID { get }

    var preferredEventLoop: EventLoop? { get }

    var deadline: NIODeadline? { get }

    func complete(with: Result<Connection, PoolError>)
}

public final class ConnectionPool<
    Factory: ConnectionFactory,
    Connection: PooledConnection,
    ConnectionID: Hashable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    Request: ConnectionRequestProtocol,
    RequestID: Hashable,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    MetricsDelegate: ConnectionPoolMetricsDelegate
>: @unchecked Sendable where
    Factory.Connection == Connection,
    Factory.ConnectionID == ConnectionID,
    Factory.ConnectionIDGenerator == ConnectionIDGenerator,
    Factory.Request == Request,
    Factory.KeepAliveBehavior == KeepAliveBehavior,
    Factory.MetricsDelegate == MetricsDelegate,
    Connection.ID == ConnectionID,
    ConnectionIDGenerator.ID == ConnectionID,
    Request.Connection == Connection,
    Request.ID == RequestID,
    KeepAliveBehavior.Connection == Connection,
    MetricsDelegate.ConnectionID == ConnectionID
{
    @usableFromInline
    typealias StateMachine = PoolStateMachine<Connection, ConnectionIDGenerator, ConnectionID, Request, Request.ID>

    public let eventLoopGroup: EventLoopGroup

    @usableFromInline
    let factory: Factory

    @usableFromInline
    let keepAliveBehavior: KeepAliveBehavior

    @usableFromInline let metricsDelegate: MetricsDelegate

    @usableFromInline
    let configuration: ConnectionPoolConfiguration

    @usableFromInline let stateLock = NIOLock()
    @usableFromInline private(set) var _stateMachine: StateMachine
    @usableFromInline private(set) var _lastConnectError: Error?
    /// The request connection timeout timers. Protected by the stateLock
    @usableFromInline private(set) var _requestTimer = [Request.ID: Scheduled<Void>]()
    /// The connection keep-alive timers. Protected by the stateLock
    @usableFromInline private(set) var _keepAliveTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection idle timers. Protected by the stateLock
    @usableFromInline private(set) var _idleTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection backoff timers. Protected by the stateLock
    @usableFromInline private(set) var _backoffTimer = [ConnectionID: Scheduled<Void>]()

    private let requestIDGenerator = PoolModule.ConnectionIDGenerator()

    public init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
        requestType: Request.Type,
        keepAliveBehavior: KeepAliveBehavior,
        metricsDelegate: MetricsDelegate,
        eventLoopGroup: EventLoopGroup
    ) {
        self.eventLoopGroup = eventLoopGroup
        self.factory = factory
        self.keepAliveBehavior = keepAliveBehavior
        self.metricsDelegate = metricsDelegate
        self.configuration = configuration
        self._stateMachine = PoolStateMachine(
            configuration: .init(configuration, keepAliveBehavior: keepAliveBehavior),
            generator: idGenerator,
            eventLoopGroup: eventLoopGroup
        )

        let connectionRequests = self._stateMachine.refillConnections()

        for request in connectionRequests {
            self.makeConnection(for: request)
        }
    }

    @inlinable
    public func releaseConnection(_ connection: Connection, streams: UInt16 = 1) {
        self.metricsDelegate.connectionReleased(id: connection.id)

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.releaseConnection(connection, streams: streams)
        }
    }

    @inlinable
    public func leaseConnection(_ request: Request) {
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.leaseConnection(request)
        }
    }

    @inlinable
    public func leaseConnections(for requests: some Collection<Request>) {
        let unlocked = self.stateLock.withLock { () -> [Actions.Unlocked] in
            var unlocked = [Actions.Unlocked]()
            unlocked.reserveCapacity(requests.count)

            for request in requests {
                let stateMachineAction = self._stateMachine.leaseConnection(request)
                let poolAction = Actions(from: stateMachineAction, lastConnectError: self._lastConnectError)
                self.runLockedConnectionAction(poolAction.locked.connection)
                self.runLockedRequestAction(poolAction.locked.request)
                unlocked.append(poolAction.unlocked)
            }

            return unlocked
        }

        for unlockedActions in unlocked {
            self.runUnlockedActions(unlockedActions)
        }
    }

    public func cancelConnectionRequest(_ requestID: RequestID) {
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.cancelRequest(id: requestID)
        }
    }

    /// Mark a connection as going away. Connection implementors have to call this method if there connection
    /// has received a close intent from the server. For example: an HTTP/2 GOWAY frame.
    public func connectionWillClose(_ connection: Connection) {

    }

    public func connectionDidClose(_ connection: Connection) {
        // TODO: Can we get access to a potential connection error here?
        self.metricsDelegate.connectionClosed(id: connection.id, error: nil)

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.connectionClosed(connection)
        }
    }

    public func connection(_ connection: Connection, didReceiveNewMaxStreamSetting: UInt16) {

    }

    public func shutdown() async throws {
        let promise = self.eventLoopGroup.any().makePromise(of: Void.self)
        self.shutdown(promise: promise)
        try await promise.futureResult.get()
    }

    public func shutdown(promise: EventLoopPromise<Void>?) {
        let promise = promise ?? self.eventLoopGroup.any().makePromise(of: Void.self)
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.forceShutdown(promise)
        }
    }

    // MARK: - Private Methods -

    // MARK: Actions

    /// An `PostgresConnectionPool` internal action type that matches the `StateMachine`'s action.
    /// However it splits up the actions into actions that need to be executed inside the `stateLock`
    /// and outside the `stateLock`.
    @usableFromInline
    struct Actions {
        @usableFromInline
        enum ConnectionAction {
            @usableFromInline
            enum Unlocked {
                case createConnection(StateMachine.ConnectionRequest)
                case closeConnection(Connection)
                case closeConnections([Connection])
                case runKeepAlive(Connection)
                case shutdownComplete(EventLoopPromise<Void>)
                case none
            }

            @usableFromInline
            enum Locked {
                case scheduleBackoffTimer(ConnectionID, backoff: TimeAmount, on: EventLoop)
                case cancelBackoffTimers([ConnectionID])

                case scheduleKeepAliveTimer(ConnectionID, on: EventLoop)
                case cancelKeepAliveTimer(ConnectionID)
                case scheduleIdleTimeoutTimer(ConnectionID, on: EventLoop)
                case cancelIdleTimeoutTimer(ConnectionID)
                case scheduleKeepAliveAndIdleTimeoutTimer(ConnectionID, on: EventLoop)
                case cancelKeepAliveAndIdleTimeoutTimer(ConnectionID)
                case cancelTimers(idle: [ConnectionID], keepAlive: [ConnectionID], backoff: [ConnectionID])
                case none
            }
        }

        @usableFromInline
        enum RequestAction {
            @usableFromInline
            enum Unlocked {
                case leaseConnection(RequestCollection<Request>, Connection)
                case failRequest(Request, PoolError)
                case failRequests(RequestCollection<Request>, PoolError)
                case none
            }

            @usableFromInline
            enum Locked {
                case scheduleRequestTimeout(for: Request, on: EventLoop)
                case cancelRequestTimeout(Request.ID)
                case cancelRequestTimeouts(LazyFilterSequence<RequestCollection<Request>>)
                case none
            }
        }

        @usableFromInline
        struct Locked {
            @usableFromInline
            var connection: ConnectionAction.Locked
            @usableFromInline
            var request: RequestAction.Locked

            @inlinable
            init(connection: ConnectionAction.Locked, request: RequestAction.Locked) {
                self.connection = connection
                self.request = request
            }
        }

        @usableFromInline
        struct Unlocked {
            @usableFromInline
            var connection: ConnectionAction.Unlocked
            @usableFromInline
            var request: RequestAction.Unlocked

            @inlinable
            init(connection: ConnectionAction.Unlocked, request: RequestAction.Unlocked) {
                self.connection = connection
                self.request = request
            }
        }

        @usableFromInline
        var locked: Locked
        @usableFromInline
        var unlocked: Unlocked

        @inlinable
        init(from stateMachineAction: StateMachine.Action, lastConnectError: Error?) {
            self.locked = Locked(connection: .none, request: .none)
            self.unlocked = Unlocked(connection: .none, request: .none)

            switch stateMachineAction.request {
            case .leaseConnection(let requests, let connection, cancelTimeout: let cancelTimeout):

                if cancelTimeout {
                    self.locked.request = .cancelRequestTimeouts(requests.lazy.filter { $0.deadline != nil })
                }
                self.unlocked.request = .leaseConnection(requests, connection)
            case .failRequest(let request, let error, cancelTimeout: let cancelTimeout):
                if cancelTimeout {
                    self.locked.request = .cancelRequestTimeout(request.id)
                }
                self.unlocked.request = .failRequest(request, error)
            case .failRequestsAndCancelTimeouts(let requests, let error):
                self.locked.request = .cancelRequestTimeouts(requests.lazy.filter { $0.deadline != nil })
                self.unlocked.request = .failRequests(requests, error)
            case .scheduleRequestTimeout(for: let request, on: let eventLoop):
                self.locked.request = .scheduleRequestTimeout(for: request, on: eventLoop)
            case .none:
                break
            }

            switch stateMachineAction.connection {
            case .createConnection(let connectionRequest):
                self.unlocked.connection = .createConnection(connectionRequest)
            case .scheduleBackoffTimer(let connectionID, backoff: let backoff, on: let eventLoop):
                self.locked.connection = .scheduleBackoffTimer(connectionID, backoff: backoff, on: eventLoop)
            case .scheduleKeepAliveTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .scheduleKeepAliveTimer(connectionID, on: eventLoop)
            case .cancelKeepAliveTimer(let connectionID):
                self.locked.connection = .cancelKeepAliveTimer(connectionID)
            case .closeConnection(let connection, let cancelKeepAliveTimer):
                if cancelKeepAliveTimer {
                    self.locked.connection = .cancelKeepAliveTimer(connection.id)
                }
                self.unlocked.connection = .closeConnection(connection)
            case .scheduleKeepAliveAndIdleTimeoutTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .scheduleKeepAliveAndIdleTimeoutTimer(connectionID, on: eventLoop)
            case .cancelKeepAliveAndIdleTimeoutTimer(let connectionID):
                self.locked.connection = .cancelKeepAliveAndIdleTimeoutTimer(connectionID)
            case .scheduleIdleTimeoutTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .scheduleIdleTimeoutTimer(connectionID, on: eventLoop)
            case .cancelIdleTimeoutTimer(let connectionID):
                self.locked.connection = .cancelIdleTimeoutTimer(connectionID)
            case .runKeepAlive(let connection):
                self.unlocked.connection = .runKeepAlive(connection)
            case .shutdown(let shutdown):
                let idleTimers = shutdown.connections.compactMap { $0.cancelIdleTimer ? $0.connection.id : nil }
                let keepAliveTimers = shutdown.connections.compactMap { $0.cancelKeepAliveTimer ? $0.connection.id : nil }
                let backoffTimers = shutdown.backoffTimersToCancel
                self.locked.connection = .cancelTimers(idle: idleTimers, keepAlive: keepAliveTimers, backoff: backoffTimers)
                self.unlocked.connection = .closeConnections(shutdown.connections.map { $0.connection })
            case .shutdownComplete(let promise):
                self.unlocked.connection = .shutdownComplete(promise)
            case .none:
                break

            }
        }
    }

    // MARK: Run actions

    @inlinable
    /*private*/ func modifyStateAndRunActions(_ closure: (inout StateMachine) -> StateMachine.Action) {
        let unlockedActions = self.stateLock.withLock { () -> Actions.Unlocked in
            let stateMachineAction = closure(&self._stateMachine)
            let poolAction = Actions(from: stateMachineAction, lastConnectError: self._lastConnectError)
            self.runLockedConnectionAction(poolAction.locked.connection)
            self.runLockedRequestAction(poolAction.locked.request)
            return poolAction.unlocked
        }
        self.runUnlockedActions(unlockedActions)
    }

    @inlinable
    /*private*/ func runLockedConnectionAction(_ action: Actions.ConnectionAction.Locked) {
        switch action {
        case .scheduleBackoffTimer(let connectionID, backoff: let backoff, on: let eventLoop):
            self.scheduleConnectionStartBackoffTimer(connectionID, backoff, on: eventLoop)

        case .scheduleKeepAliveTimer(let connectionID, on: let eventLoop):
            self.scheduleKeepAliveTimerForConnection(connectionID, on: eventLoop)

        case .cancelKeepAliveTimer(let connectionID):
            self.cancelKeepAliveTimerForConnection(connectionID)

        case .scheduleKeepAliveAndIdleTimeoutTimer(let connectionID, on: let eventLoop):
            self.scheduleKeepAliveTimerForConnection(connectionID, on: eventLoop)
            self.scheduleIdleTimeoutTimerForConnection(connectionID, on: eventLoop)

        case .cancelKeepAliveAndIdleTimeoutTimer(let connectionID):
            self.cancelKeepAliveTimerForConnection(connectionID)
            self.cancelIdleTimeoutTimerForConnection(connectionID)

        case .scheduleIdleTimeoutTimer(let connectionID, on: let eventLoop):
            self.scheduleIdleTimeoutTimerForConnection(connectionID, on: eventLoop)

        case .cancelIdleTimeoutTimer(let connectionID):
            self.cancelIdleTimeoutTimerForConnection(connectionID)

        case .cancelBackoffTimers(let connectionIDs):
            for connectionID in connectionIDs {
                self.cancelConnectionStartBackoffTimer(connectionID)
            }

        case .cancelTimers(let idleTimers, let keepAliveTimers, let backoffTimers):
            idleTimers.forEach { self.cancelIdleTimeoutTimerForConnection($0) }
            keepAliveTimers.forEach { self.cancelKeepAliveTimerForConnection($0) }
            backoffTimers.forEach { self.cancelConnectionStartBackoffTimer($0) }

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func runLockedRequestAction(_ action: Actions.RequestAction.Locked) {
        switch action {
        case .scheduleRequestTimeout(for: let request, on: let eventLoop):
            self.scheduleRequestTimeout(request, on: eventLoop)

        case .cancelRequestTimeout(let requestID):
            self.cancelRequestTimeout(requestID)

        case .cancelRequestTimeouts(let requests):
            requests.forEach { self.cancelRequestTimeout($0.id) }

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func runUnlockedActions(_ actions: Actions.Unlocked) {
        self.runUnlockedConnectionAction(actions.connection)
        self.runUnlockedRequestAction(actions.request)
    }

    @inlinable
    /*private*/ func runUnlockedConnectionAction(_ action: Actions.ConnectionAction.Unlocked) {
        switch action {
        case .createConnection(let connectionRequest):
            self.makeConnection(for: connectionRequest)

        case .closeConnection(let connection):
            self.closeConnection(connection)

        case .closeConnections(let connections):
            connections.forEach { self.closeConnection($0) }

        case .runKeepAlive(let connection):
            self.runKeepAlive(connection)

        case .shutdownComplete(let promise):
            promise.succeed(())

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func runUnlockedRequestAction(_ action: Actions.RequestAction.Unlocked) {
        switch action {
        case .leaseConnection(let requests, let connection):
            self.metricsDelegate.connectionLeased(id: connection.id)
            for request in requests {
                request.complete(with: .success(connection))
            }
        case .failRequest(let request, let error):
            request.complete(with: .failure(error))
        case .failRequests(let requests, let error):
            for request in requests { request.complete(with: .failure(error)) }
        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func makeConnection(for request: StateMachine.ConnectionRequest) {
        self.metricsDelegate.startedConnecting(id: request.connectionID)

        self.factory.makeConnection(
            on: request.eventLoop,
            id: request.connectionID,
            for: self
        ).whenComplete { result in
            switch result {
            case .success(let connectionBundle):
                self.connectionEstablished(connectionBundle)
                connectionBundle.connection.onClose {
                    self.connectionDidClose(connectionBundle.connection)
                }
            case .failure(let error):
                self.connectionEstablishFailed(error, for: request)
            }
        }
    }

    @inlinable
    /*private*/ func connectionEstablished(_ connectionBundle: ConnectionAndMetadata<Connection>) {
        self.metricsDelegate.connectSucceeded(id: connectionBundle.connection.id)

        self.modifyStateAndRunActions { stateMachine in
            self._lastConnectError = nil
            return stateMachine.connectionEstablished(
                connectionBundle.connection,
                maxStreams: connectionBundle.maximalStreamsOnConnection
            )
        }
    }

    @inlinable
    /*private*/ func connectionEstablishFailed(_ error: Error, for request: StateMachine.ConnectionRequest) {
        self.metricsDelegate.connectFailed(id: request.connectionID, error: error)

        self.modifyStateAndRunActions { stateMachine in
            self._lastConnectError = error
            return stateMachine.connectionEstablishFailed(error, for: request)
        }
    }

    @inlinable
    /*private*/ func scheduleRequestTimeout(_ request: Request, on eventLoop: EventLoop) {
        let requestID = request.id
        let scheduled = eventLoop.scheduleTask(deadline: request.deadline!) {
            // there might be a race between a the timeout timer and the pool scheduling the
            // request on another thread.
            self.modifyStateAndRunActions { stateMachine in
                if self._requestTimer.removeValue(forKey: requestID) != nil {
                    // The timer still exists. State Machines assumes it is alive. Inform state
                    // machine.
                    return stateMachine.timeoutRequest(id: requestID)
                }
                return .none()
            }
        }

        assert(self._requestTimer[requestID] == nil)
        self._requestTimer[requestID] = scheduled
    }

    @inlinable
    /*private*/ func cancelRequestTimeout(_ id: Request.ID) {
        guard let cancelTimer = self._requestTimer.removeValue(forKey: id) else {
            preconditionFailure("Expected to have a timer for request \(id) at this point.")
        }
        cancelTimer.cancel()
    }

    @inlinable
    /*private*/ func scheduleKeepAliveTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
        let scheduled = eventLoop.scheduleTask(in: self.keepAliveBehavior.keepAliveFrequency!) {
            // there might be a race between a cancelTimer call and the triggering
            // of this scheduled task. both want to acquire the lock
            self.modifyStateAndRunActions { stateMachine in
                if self._keepAliveTimer.removeValue(forKey: connectionID) != nil {
                    // The timer still exists. State Machines assumes it is alive
                    return stateMachine.connectionKeepAliveTimerTriggered(connectionID, on: eventLoop)
                }
                return .none()
            }
        }

        assert(self._keepAliveTimer[connectionID] == nil)
        self._keepAliveTimer[connectionID] = scheduled
    }

    @inlinable
    /*private*/ func cancelKeepAliveTimerForConnection(_ connectionID: ConnectionID) {
        guard let cancelTimer = self._keepAliveTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    @inlinable
    /*private*/ func scheduleIdleTimeoutTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
        let scheduled = eventLoop.scheduleTask(in: self.configuration.idleTimeout) {
            // there might be a race between a cancelTimer call and the triggering
            // of this scheduled task. both want to acquire the lock
            self.modifyStateAndRunActions { stateMachine in
                if self._idleTimer.removeValue(forKey: connectionID) != nil {
                    // The timer still exists. State Machines assumes it is alive
                    return stateMachine.connectionIdleTimerTriggered(connectionID, on: eventLoop)
                }
                return .none()
            }
        }

        assert(self._idleTimer[connectionID] == nil)
        self._idleTimer[connectionID] = scheduled
    }

    @inlinable
    /*private*/ func cancelIdleTimeoutTimerForConnection(_ connectionID: ConnectionID) {
        guard let cancelTimer = self._idleTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    @inlinable
    /*private*/ func scheduleConnectionStartBackoffTimer(
        _ connectionID: ConnectionID,
        _ timeAmount: TimeAmount,
        on eventLoop: EventLoop
    ) {
        let scheduled = eventLoop.scheduleTask(in: timeAmount) {
            // there might be a race between a backoffTimer and the pool shutting down.
            self.modifyStateAndRunActions { stateMachine in
                if self._backoffTimer.removeValue(forKey: connectionID) != nil {
                    // The timer still exists. State Machines assumes it is alive
                    return stateMachine.connectionCreationBackoffDone(connectionID, on: eventLoop)
                }
                return .none()
            }
        }

        assert(self._backoffTimer[connectionID] == nil)
        self._backoffTimer[connectionID] = scheduled
    }

    @inlinable
    /*private*/ func cancelConnectionStartBackoffTimer(_ connectionID: ConnectionID) {
        guard let backoffTimer = self._backoffTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have a backoff timer for connection \(connectionID) at this point.")
        }
        backoffTimer.cancel()
    }

    @inlinable
    /*private*/ func runKeepAlive(_ connection: Connection) {
        self.metricsDelegate.keepAliveTriggered(id: connection.id)

        self.keepAliveBehavior.runKeepAlive(for: connection).whenComplete { result in
            switch result {
            case .success:
                self.metricsDelegate.keepAliveSucceeded(id: connection.id)

                self.modifyStateAndRunActions { stateMachine in
                    stateMachine.connectionKeepAliveDone(connection)
                }
            case .failure(let error):
                self.metricsDelegate.keepAliveFailed(id: connection.id, error: error)

                self.modifyStateAndRunActions { stateMachine in
                    stateMachine.connectionClosed(connection)
                }
            }
        }
    }

    @inlinable
    /*private*/ func closeConnection(_ connection: Connection) {
        self.metricsDelegate.connectionClosing(id: connection.id)

        connection.close()
    }
}

extension PoolConfiguration {
    init<KeepAliveBehavior: ConnectionKeepAliveBehavior>(_ configuration: ConnectionPoolConfiguration, keepAliveBehavior: KeepAliveBehavior) {
        self.minimumConnectionCount = configuration.minimumConnectionCount
        self.maximumConnectionSoftLimit = configuration.maximumConnectionSoftLimit
        self.maximumConnectionHardLimit = configuration.maximumConnectionHardLimit
        self.keepAlive = keepAliveBehavior.keepAliveFrequency != nil
    }
}
