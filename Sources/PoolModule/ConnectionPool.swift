import NIOCore
import NIOConcurrencyHelpers

public protocol ConnectionKeepAliveBehavior: Sendable {
    associatedtype Connection: PooledConnection

    var keepAliveFrequency: TimeAmount? { get }

    func runKeepAlive(for connection: Connection) -> EventLoopFuture<Void>
}

public protocol ConnectionFactory {
    associatedtype ConnectionID: Hashable
    associatedtype Connection

    func makeConnection(
        on eventLoop: EventLoop,
        id: ConnectionID
    ) -> EventLoopFuture<Connection>
}

public struct ConnectionPoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the Redis servers close these idle connections,
    /// the `RedisConnectionPool` will initiate new outbound connections proactively to avoid the number of available connections dropping below this number.
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

public final class ConnectionPool<
    Factory: ConnectionFactory,
    Connection: PooledConnection,
    ConnectionID: Hashable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    MetricsDelegate: ConnectionPoolMetricsDelegate
>: @unchecked Sendable where Factory.Connection == Connection, Factory.ConnectionID == ConnectionID, Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, KeepAliveBehavior.Connection == Connection, MetricsDelegate.ConnectionID == ConnectionID {
    
    typealias StateMachine = PoolStateMachine<Connection, ConnectionIDGenerator, ConnectionID, Request, Request.ID>

    public let eventLoopGroup: EventLoopGroup

    let factory: Factory

    let keepAliveBehavior: KeepAliveBehavior

    let metricsDelegate: MetricsDelegate

    let configuration: ConnectionPoolConfiguration

    private let stateLock = NIOLock()
    private var _stateMachine: StateMachine
    private var _lastConnectError: Error?
    /// The request connection timeout timers. Protected by the stateLock
    private var _requestTimer = [Request.ID: Scheduled<Void>]()
    /// The connection keep-alive timers. Protected by the stateLock
    private var _keepAliveTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection idle timers. Protected by the stateLock
    private var _idleTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection backoff timers. Protected by the stateLock
    private var _backoffTimer = [ConnectionID: Scheduled<Void>]()

    private let requestIDGenerator = PoolModule.ConnectionIDGenerator()

    public init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
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

    public func leaseConnection() async throws -> Connection {
        let requestID = self.requestIDGenerator.next()

        let connection = try await withTaskCancellationHandler {
            if Task.isCancelled {
                throw CancellationError()
            }

            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Connection, Error>) in
                let request = Request(
                    id: requestID,
                    deadline: .now() + .seconds(10),
                    continuation: continuation,
                    preferredEventLoop: nil
                )

                self.modifyStateAndRunActions { stateMachine in
                    stateMachine.leaseConnection(request)
                }
            }
        } onCancel: {
            self.modifyStateAndRunActions { stateMachine in
                stateMachine.cancelRequest(id: requestID)
            }
        }

        self.metricsDelegate.connectionLeased(id: connection.id)

        return connection
    }

    public func leaseConnection(preferredEventLoop: EventLoop) -> EventLoopFuture<Connection> {
        let requestID = self.requestIDGenerator.next()

        let promise = preferredEventLoop.makePromise(of: Connection.self)

        let request = Request(
            id: requestID,
            deadline: .now() + .seconds(10),
            promise: promise,
            preferredEventLoop: preferredEventLoop
        )

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.leaseConnection(request)
        }

        return promise.futureResult
    }

    public func releaseConnection(_ connection: Connection) {
        self.metricsDelegate.connectionReleased(id: connection.id)

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.releaseConnection(connection)
        }
    }

    public func withConnection<Result>(
        preferredEventLoop: EventLoop,
        _ closure: @escaping @Sendable (Connection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {

        return self.leaseConnection(preferredEventLoop: preferredEventLoop).flatMap { connection in
            let returnedFuture = closure(connection)
            returnedFuture.whenComplete { _ in
                self.releaseConnection(connection)
            }
            return returnedFuture
        }
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
    private struct Actions {
        enum ConnectionAction {
            enum Unlocked {
                case createConnection(StateMachine.ConnectionRequest)
                case closeConnection(Connection)
                case closeConnections([Connection])
                case runKeepAlive(Connection)
                case shutdownComplete(EventLoopPromise<Void>)
                case none
            }

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

        enum RequestAction {
            enum Unlocked {
                case leaseConnection(Request, Connection)
                case failRequest(Request, Error)
                case failRequests([Request], Error)
                case none
            }

            enum Locked {
                case scheduleRequestTimeout(for: Request, on: EventLoop)
                case cancelRequestTimeout(Request.ID)
                case cancelRequestTimeouts([Request])
                case none
            }
        }

        struct Locked {
            var connection: ConnectionAction.Locked
            var request: RequestAction.Locked
        }

        struct Unlocked {
            var connection: ConnectionAction.Unlocked
            var request: RequestAction.Unlocked
        }

        var locked: Locked
        var unlocked: Unlocked

        init(from stateMachineAction: StateMachine.Action, lastConnectError: Error?) {
            self.locked = Locked(connection: .none, request: .none)
            self.unlocked = Unlocked(connection: .none, request: .none)

            switch stateMachineAction.request {
            case .leaseConnection(let request, let connection, cancelTimeout: let cancelTimeout):
                if cancelTimeout {
                    self.locked.request = .cancelRequestTimeout(request.id)
                }
                self.unlocked.request = .leaseConnection(request, connection)
            case .failRequest(let request, let error, cancelTimeout: let cancelTimeout):
                if cancelTimeout {
                    self.locked.request = .cancelRequestTimeout(request.id)
                }
                self.unlocked.request = .failRequest(request, error)
            case .failRequestsAndCancelTimeouts(let requests, let error):
                self.locked.request = .cancelRequestTimeouts(requests)
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

    private func modifyStateAndRunActions(_ closure: (inout StateMachine) -> StateMachine.Action) {
        let unlockedActions = self.stateLock.withLock { () -> Actions.Unlocked in
            let stateMachineAction = closure(&self._stateMachine)
            let poolAction = Actions(from: stateMachineAction, lastConnectError: self._lastConnectError)
            self.runLockedConnectionAction(poolAction.locked.connection)
            self.runLockedRequestAction(poolAction.locked.request)
            return poolAction.unlocked
        }
        self.runUnlockedActions(unlockedActions)
    }

    private func runLockedConnectionAction(_ action: Actions.ConnectionAction.Locked) {
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

    private func runLockedRequestAction(_ action: Actions.RequestAction.Locked) {
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

    private func runUnlockedActions(_ actions: Actions.Unlocked) {
        self.runUnlockedConnectionAction(actions.connection)
        self.runUnlockedRequestAction(actions.request)
    }

    private func runUnlockedConnectionAction(_ action: Actions.ConnectionAction.Unlocked) {
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

    private func runUnlockedRequestAction(_ action: Actions.RequestAction.Unlocked) {
        switch action {
        case .leaseConnection(let request, let connection):
            self.metricsDelegate.connectionLeased(id: connection.id)
            request.succeed(connection)
        case .failRequest(let request, let error):
            request.fail(error)
        case .failRequests(let requests, let error):
            for request in requests { request.fail(error) }
        case .none:
            break
        }
    }

    private func makeConnection(for request: StateMachine.ConnectionRequest) {
        self.metricsDelegate.startedConnecting(id: request.connectionID)

        self.factory.makeConnection(
            on: request.eventLoop,
            id: request.connectionID
        ).whenComplete { result in
            switch result {
            case .success(let connection):
                self.connectionEstablished(connection)
                connection.onClose {
                    self.connectionClosed(connection)
                }
            case .failure(let error):
                self.connectionEstablishFailed(error, for: request)
            }
        }
    }

    private func connectionEstablished(_ connection: Connection) {
        self.metricsDelegate.connectSucceeded(id: connection.id)

        self.modifyStateAndRunActions { stateMachine in
            self._lastConnectError = nil
            return stateMachine.connectionEstablished(connection)
        }
    }

    private func connectionEstablishFailed(_ error: Error, for request: StateMachine.ConnectionRequest) {
        self.metricsDelegate.connectFailed(id: request.connectionID, error: error)

        self.modifyStateAndRunActions { stateMachine in
            self._lastConnectError = error
            return stateMachine.connectionEstablishFailed(error, for: request)
        }
    }

    private func scheduleRequestTimeout(_ request: Request, on eventLoop: EventLoop) {
        let requestID = request.id
        let scheduled = eventLoop.scheduleTask(deadline: request.deadline) {
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

    private func cancelRequestTimeout(_ id: Request.ID) {
        guard let cancelTimer = self._requestTimer.removeValue(forKey: id) else {
            preconditionFailure("Expected to have a timer for request \(id) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleKeepAliveTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
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

    private func cancelKeepAliveTimerForConnection(_ connectionID: ConnectionID) {
        guard let cancelTimer = self._keepAliveTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleIdleTimeoutTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
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

    private func cancelIdleTimeoutTimerForConnection(_ connectionID: ConnectionID) {
        guard let cancelTimer = self._idleTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleConnectionStartBackoffTimer(
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

    private func cancelConnectionStartBackoffTimer(_ connectionID: ConnectionID) {
        guard let backoffTimer = self._backoffTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have a backoff timer for connection \(connectionID) at this point.")
        }
        backoffTimer.cancel()
    }

    private func runKeepAlive(_ connection: Connection) {
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

    private func closeConnection(_ connection: Connection) {
        self.metricsDelegate.connectionClosing(id: connection.id)

        connection.close()
    }

    private func connectionClosed(_ connection: Connection) {
        // TODO: Can we get access to a potential connection error here?
        self.metricsDelegate.connectionClosed(id: connection.id, error: nil)

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.connectionClosed(connection)
        }
    }
}

extension ConnectionPool {
    struct Request: ConnectionRequest {
        private enum AsyncReportingMechanism {
            case continuation(CheckedContinuation<Connection, Error>)
            case promise(EventLoopPromise<Connection>)
        }

        typealias ID = Int

        var id: ID

        var preferredEventLoop: NIOCore.EventLoop?

        var deadline: NIOCore.NIODeadline

        private var reportingMechanism: AsyncReportingMechanism

        init(
            id: Int,
            deadline: NIOCore.NIODeadline,
            continuation: CheckedContinuation<Connection, Error>,
            preferredEventLoop: EventLoop?
        ) {
            self.id = id
            self.deadline = deadline
            self.preferredEventLoop = preferredEventLoop
            self.reportingMechanism = .continuation(continuation)
        }

        init(
            id: Int,
            deadline: NIOCore.NIODeadline,
            promise: EventLoopPromise<Connection>,
            preferredEventLoop: EventLoop?
        ) {
            self.id = id
            self.deadline = deadline
            self.preferredEventLoop = preferredEventLoop
            self.reportingMechanism = .promise(promise)
        }

        fileprivate func succeed(_ connection: Connection) {
            switch self.reportingMechanism {
            case .continuation(let continuation):
                continuation.resume(returning: connection)
            case .promise(let promise):
                promise.succeed(connection)
            }
        }

        fileprivate func fail(_ error: Error) {
            switch self.reportingMechanism {
            case .continuation(let continuation):
                continuation.resume(throwing: error)
            case .promise(let promise):
                promise.fail(error)
            }
        }
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
