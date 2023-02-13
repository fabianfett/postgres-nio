import NIOCore
import NIOConcurrencyHelpers
import Logging

public protocol ConnectionKeepAliveBehavior: Sendable {
    associatedtype Connection: PooledConnection

    var keepAliveFrequency: TimeAmount? { get }

    func runKeepAlive(for connection: Connection, logger: Logger) -> EventLoopFuture<Void>
}

public protocol ConnectionFactory {
    associatedtype ConnectionID: Hashable
    associatedtype Connection

    func makeConnection(
        on eventLoop: EventLoop,
        id: ConnectionID,
        backgroundLogger: Logger
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

public protocol ConnectionRequest {
    associatedtype ID: Hashable

    var id: ID { get }

    var preferredEventLoop: EventLoop? { get }

    var deadline: NIODeadline { get }
}

public final class ConnectionPool<
    Factory: ConnectionFactory,
    Connection: PooledConnection,
    ConnectionID: Hashable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    MetricsDelegate: ConnectionPoolMetricsDelegate
>: @unchecked Sendable where Factory.Connection == Connection, Factory.ConnectionID == ConnectionID, Connection.ID == ConnectionID, ConnectionIDGenerator.ID == ConnectionID, KeepAliveBehavior.Connection == Connection {
    
    typealias StateMachine = PoolStateMachine<Connection, ConnectionIDGenerator, ConnectionID, Request, Request.ID>

    let eventLoopGroup: EventLoopGroup

    let factory: Factory

    let keepAliveBehavior: KeepAliveBehavior

    let metricsDelegate: MetricsDelegate

    let backgroundLogger: Logger

    let configuration: ConnectionPoolConfiguration

    private let stateLock = NIOLock()
    private var _stateMachine: StateMachine
    private var _lastConnectError: Error?
    /// The request connection timeout timers. Protected by the stateLock
    private var _requestTimer = [Request.ID: Scheduled<Void>]()
    /// The connection ping timers. Protected by the stateLock
    private var _pingTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection idle timers. Protected by the stateLock
    private var _idleTimer = [ConnectionID: Scheduled<Void>]()
    /// The connection backoff timers. Protected by the stateLock
    private var _backoffTimer = [ConnectionID: Scheduled<Void>]()

    private let requestIDGenerator = PostgresNIO.ConnectionIDGenerator()

    public init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
        keepAliveBehavior: KeepAliveBehavior,
        metricsDelegate: MetricsDelegate,
        eventLoopGroup: EventLoopGroup,
        backgroundLogger: Logger
    ) {
        self.eventLoopGroup = eventLoopGroup
        self.factory = factory
        self.keepAliveBehavior = keepAliveBehavior
        self.metricsDelegate = metricsDelegate
        self.backgroundLogger = backgroundLogger
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

    public func leaseConnection(logger: Logger) async throws -> Connection {
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

        logger.debug("leased connection", metadata: [
            PSQLLoggingMetadata.Key.connectionID.rawValue: "\(connection.id)",
            PSQLLoggingMetadata.Key.requestID.rawValue: "\(requestID)",
        ])

        return connection
    }

    public func leaseConnection(logger: Logger, preferredEventLoop: EventLoop) -> EventLoopFuture<Connection> {
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
        self.backgroundLogger.debug("connection released", metadata: [
            PSQLLoggingMetadata.Key.connectionID.rawValue: "\(connection.id)",
        ])

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.releaseConnection(connection)
        }
    }

    public func withConnection<Result>(
        logger: Logger,
        preferredEventLoop: EventLoop,
        _ closure: @escaping @Sendable (Connection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {

        return self.leaseConnection(logger: logger, preferredEventLoop: preferredEventLoop).flatMap { connection in
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
                case runPingPong(Connection)
                case shutdownComplete(EventLoopPromise<Void>)
                case none
            }

            enum Locked {
                case scheduleBackoffTimer(ConnectionID, backoff: TimeAmount, on: EventLoop)
                case cancelBackoffTimers([ConnectionID])

                case schedulePingTimer(ConnectionID, on: EventLoop)
                case cancelPingTimer(ConnectionID)
                case scheduleIdleTimeoutTimer(ConnectionID, on: EventLoop)
                case cancelIdleTimeoutTimer(ConnectionID)
                case schedulePingAndIdleTimeoutTimer(ConnectionID, on: EventLoop)
                case cancelPingAndIdleTimeoutTimer(ConnectionID)
                case cancelTimers(idle: [ConnectionID], pingPong: [ConnectionID], backoff: [ConnectionID])
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
            case .schedulePingTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .schedulePingTimer(connectionID, on: eventLoop)
            case .cancelPingTimer(let connectionID):
                self.locked.connection = .cancelPingTimer(connectionID)
            case .closeConnection(let connection, let cancelPingPongTimer):
                if cancelPingPongTimer {
                    self.locked.connection = .cancelPingTimer(connection.id)
                }
                self.unlocked.connection = .closeConnection(connection)
            case .schedulePingAndIdleTimeoutTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .schedulePingAndIdleTimeoutTimer(connectionID, on: eventLoop)
            case .cancelPingAndIdleTimeoutTimer(let connectionID):
                self.locked.connection = .cancelPingAndIdleTimeoutTimer(connectionID)
            case .scheduleIdleTimeoutTimer(let connectionID, on: let eventLoop):
                self.locked.connection = .scheduleIdleTimeoutTimer(connectionID, on: eventLoop)
            case .cancelIdleTimeoutTimer(let connectionID):
                self.locked.connection = .cancelIdleTimeoutTimer(connectionID)
            case .runPingPong(let connection):
                self.unlocked.connection = .runPingPong(connection)
            case .shutdown(let shutdown):
                let idleTimers = shutdown.connections.compactMap { $0.cancelIdleTimer ? $0.connection.id : nil }
                let pingPongTimers = shutdown.connections.compactMap { $0.cancelPingPongTimer ? $0.connection.id : nil }
                let backoffTimers = shutdown.backoffTimersToCancel
                self.locked.connection = .cancelTimers(idle: idleTimers, pingPong: pingPongTimers, backoff: backoffTimers)
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

        case .schedulePingTimer(let connectionID, on: let eventLoop):
            self.schedulePingTimerForConnection(connectionID, on: eventLoop)

        case .cancelPingTimer(let connectionID):
            self.cancelPingTimerForConnection(connectionID)

        case .schedulePingAndIdleTimeoutTimer(let connectionID, on: let eventLoop):
            self.schedulePingTimerForConnection(connectionID, on: eventLoop)
            self.scheduleIdleTimeoutTimerForConnection(connectionID, on: eventLoop)

        case .cancelPingAndIdleTimeoutTimer(let connectionID):
            self.cancelPingTimerForConnection(connectionID)
            self.cancelIdleTimeoutTimerForConnection(connectionID)

        case .scheduleIdleTimeoutTimer(let connectionID, on: let eventLoop):
            self.scheduleIdleTimeoutTimerForConnection(connectionID, on: eventLoop)

        case .cancelIdleTimeoutTimer(let connectionID):
            self.cancelIdleTimeoutTimerForConnection(connectionID)

        case .cancelBackoffTimers(let connectionIDs):
            for connectionID in connectionIDs {
                self.cancelConnectionStartBackoffTimer(connectionID)
            }

        case .cancelTimers(let idleTimers, let pingPongTimers, let backoffTimers):
            idleTimers.forEach { self.cancelIdleTimeoutTimerForConnection($0) }
            pingPongTimers.forEach { self.cancelPingTimerForConnection($0) }
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

        case .runPingPong(let connection):
            self.backgroundLogger.debug("run ping pong", metadata: [
                PSQLConnection.LoggerMetaDataKey.connectionID.rawValue: "\(connection.id)",
            ])

            self.keepAliveBehavior.runKeepAlive(for: connection, logger: self.backgroundLogger).whenComplete { result in
                switch result {
                case .success:
                    self.modifyStateAndRunActions { stateMachine in
                        stateMachine.connectionPingPongDone(connection)
                    }
                case .failure(let error):
                    self.modifyStateAndRunActions { stateMachine in
                        stateMachine.connectionClosed(connection)
                    }
                }
            }

        case .shutdownComplete(let promise):
            promise.succeed(())

        case .none:
            break
        }
    }

    private func runUnlockedRequestAction(_ action: Actions.RequestAction.Unlocked) {
        switch action {
        case .leaseConnection(let request, let connection):
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
        self.backgroundLogger.debug("Creating new connection", metadata: [
            .connectionID: "\(request.connectionID)",
        ])

        self.factory.makeConnection(
            on: request.eventLoop,
            id: request.connectionID,
            backgroundLogger: self.backgroundLogger
        ).whenComplete { result in
            switch result {
            case .success(let connection):
                self.connectionEstablished(connection)
                connection.onClose {
                    self.modifyStateAndRunActions { stateMachine in
                        stateMachine.connectionClosed(connection)
                    }
                }
            case .failure(let error):
                self.connectionEstablishFailed(error, for: request)
            }
        }
    }

    private func connectionEstablished(_ connection: Connection) {
        self.backgroundLogger.debug("Connection established", metadata: [
            .connectionID: "\(connection.id)"
        ])
        self.modifyStateAndRunActions { stateMachine in
            self._lastConnectError = nil
            return stateMachine.connectionEstablished(connection)
        }
    }

    private func connectionEstablishFailed(_ error: Error, for request: StateMachine.ConnectionRequest) {
        self.backgroundLogger.debug("Connection creation failed", metadata: [
            .connectionID: "\(request.connectionID)", .error: "\(error)"
        ])
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

    private func schedulePingTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
        self.backgroundLogger.trace("Schedule connection ping timer", metadata: [
            .connectionID: "\(connectionID)",
        ])
        let scheduled = eventLoop.scheduleTask(in: self.keepAliveBehavior.keepAliveFrequency!) {
            // there might be a race between a cancelTimer call and the triggering
            // of this scheduled task. both want to acquire the lock
            self.modifyStateAndRunActions { stateMachine in
                if self._pingTimer.removeValue(forKey: connectionID) != nil {
                    // The timer still exists. State Machines assumes it is alive
                    return stateMachine.connectionPingTimerTriggered(connectionID, on: eventLoop)
                }
                return .none()
            }
        }

        assert(self._pingTimer[connectionID] == nil)
        self._pingTimer[connectionID] = scheduled
    }

    private func cancelPingTimerForConnection(_ connectionID: ConnectionID) {
        self.backgroundLogger.trace("Cancel connection ping timer", metadata: [
            .connectionID: "\(connectionID)",
        ])
        guard let cancelTimer = self._pingTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleIdleTimeoutTimerForConnection(_ connectionID: ConnectionID, on eventLoop: EventLoop) {
        self.backgroundLogger.trace("Schedule idle connection timeout timer", metadata: [
            .connectionID: "\(connectionID)",
        ])
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
        self.backgroundLogger.trace("Cancel idle connection timeout", metadata: [
            .connectionID: "\(connectionID)",
        ])
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
        self.backgroundLogger.trace("Schedule connection creation backoff timer", metadata: [
            .connectionID: "\(connectionID)",
        ])

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

    private func closeConnection(_ connection: Connection) {
        self.backgroundLogger.debug("close connection", metadata: [
            PSQLConnection.LoggerMetaDataKey.connectionID.rawValue: "\(connection.id)",
        ])

        connection.close()
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
