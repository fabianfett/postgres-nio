#if swift(>=5.7)

import NIOCore
import NIOConcurrencyHelpers
import Logging

public protocol PostgresConnectionFactory {

    func makeConnection(
        on eventLoop: any EventLoop,
        id: PostgresConnection.ID,
        backgroundLogger: Logger
    ) -> EventLoopFuture<PostgresConnection>

}

@available(macOS 13.0, iOS 16.0, *)
public struct PostgresConnectionPoolConfiguration {
    /// The minimum number of connections to preserve in the pool.
    ///
    /// If the pool is mostly idle and the Redis servers close these idle connections,
    /// the `RedisConnectionPool` will initiate new outbound connections proactively to avoid the number of available connections dropping below this number.
    public var minimumConnectionCount: Int

    /// The maximum number of connections to for this pool, to be preserved.
    public var maximumConnectionSoftLimit: Int

    public var maximumConnectionHardLimit: Int

    public var pingFrequency: TimeAmount

    public var pingQuery: String

    public var idleTimeout: TimeAmount

    public init() {
        self.minimumConnectionCount = System.coreCount
        self.maximumConnectionSoftLimit = System.coreCount
        self.maximumConnectionHardLimit = System.coreCount * 4
        self.pingFrequency = .seconds(30)
        self.pingQuery = "SELECT 1;"
        self.idleTimeout = .seconds(60)
    }
}

@available(macOS 13.0, iOS 16.0, *)
public final class PostgresConnectionPool<Factory: PostgresConnectionFactory>: @unchecked Sendable {
    typealias StateMachine = PoolStateMachine<PostgresConnection, PostgresConnection.ID.Generator, PostgresConnection.ID, Request, Int>

    let eventLoopGroup: any EventLoopGroup

    let factory: Factory

    let backgroundLogger: Logger

    let configuration: PostgresConnectionPoolConfiguration

    private let stateLock = NIOLock()
    private var _stateMachine: StateMachine
    /// The request connection timeout timers. Protected by the stateLock
    private var _requestTimer = [Request.ID: Scheduled<Void>]()
    /// The connection ping timers. Protected by the stateLock
    private var _pingTimer = [PostgresConnection.ID: Scheduled<Void>]()
    /// The connection idle timers. Protected by the stateLock
    private var _idleTimer = [PostgresConnection.ID: Scheduled<Void>]()
    /// The connection backoff timers. Protected by the stateLock
    private var _backoffTimer = [PostgresConnection.ID: Scheduled<Void>]()

    private let requestIDGenerator = PostgresConnection.ID.Generator()

    public init(
        configuration: PostgresConnectionPoolConfiguration,
        factory: Factory,
        eventLoopGroup: any EventLoopGroup,
        backgroundLogger: Logger
    ) {
        self.eventLoopGroup = eventLoopGroup
        self.factory = factory
        self.backgroundLogger = backgroundLogger
        self.configuration = configuration
        self._stateMachine = PoolStateMachine(
            configuration: configuration,
            generator: PostgresConnection.ID.Generator(),
            eventLoopGroup: eventLoopGroup
        )

        let connectionRequests = self._stateMachine.refillConnections()

        for request in connectionRequests {
            self.makeConnection(for: request)
        }
    }

    public func query<Clock: _Concurrency.Clock>(
        _ query: PostgresQuery,
        deadline: Clock.Instant,
        clock: Clock,
        logger: Logger,
        file: String = #file,
        line: Int = #line
    ) async throws -> PostgresRowSequence {
        let connection = try await self.leaseConnection(logger: logger)

        return try await connection.query(query, logger: logger)
    }

    public func withConnection<Result>(logger: Logger, _ closure: (PostgresConnection) async throws -> Result) async throws -> Result {
        let connection = try await self.leaseConnection(logger: logger)

        defer { self.releaseConnection(connection) }

        return try await closure(connection)
    }

    private func leaseConnection(logger: Logger) async throws -> PostgresConnection {
        let requestID = self.requestIDGenerator.next()

        let connection = try await withTaskCancellationHandler {
            if Task.isCancelled {
                throw CancellationError()
            }

            return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<PostgresConnection, any Error>) in
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

    private func releaseConnection(_ connection: PostgresConnection) {
        self.backgroundLogger.debug("connection released", metadata: [
            PSQLLoggingMetadata.Key.connectionID.rawValue: "\(connection.id)",
        ])

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.releaseConnection(connection)
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
                case closeConnection(PostgresConnection)
                case closeConnections([PostgresConnection])
                case runPingPong(PostgresConnection)
                case shutdownComplete(EventLoopPromise<Void>)
                case none
            }

            enum Locked {
                case scheduleBackoffTimer(PostgresConnection.ID, backoff: TimeAmount, on: any EventLoop)
                case cancelBackoffTimers([PostgresConnection.ID])

                case schedulePingTimer(PostgresConnection.ID, on: any EventLoop)
                case cancelPingTimer(PostgresConnection.ID)
                case schedulePingAndIdleTimeoutTimer(PostgresConnection.ID, on: any EventLoop)
                case cancelPingAndIdleTimeoutTimer(PostgresConnection.ID)
                case cancelIdleTimeoutTimer(PostgresConnection.ID)
                case cancelTimers(idle: [PostgresConnection.ID], pingPong: [PostgresConnection.ID], backoff: [PostgresConnection.ID])
                case none
            }
        }

        enum RequestAction {
            enum Unlocked {
                case leaseConnection(Request, PostgresConnection)
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

        init(from stateMachineAction: StateMachine.Action) {
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
            let poolAction = Actions(from: stateMachineAction)
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

            connection.query(.init(unsafeSQL: self.configuration.pingQuery), logger: self.backgroundLogger).whenComplete { result in
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
                connection.closeFuture.whenComplete { _ in
                    self.modifyStateAndRunActions { stateMachine in
                        stateMachine.connectionClosed(connection)
                    }
                }
            case .failure(let error):
                self.connectionEstablishFailed(error, for: request)
            }
        }
    }

    private func connectionEstablished(_ connection: PostgresConnection) {
        self.backgroundLogger.debug("Connection established", metadata: [
            .connectionID: "\(connection.id)"
        ])
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.connectionEstablished(connection)
        }
    }

    private func connectionEstablishFailed(_ error: any Error, for request: StateMachine.ConnectionRequest) {
        self.backgroundLogger.debug("Connection creation failed", metadata: [
            .connectionID: "\(request.connectionID)", .error: "\(error)"
        ])
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.connectionEstablishFailed(error, for: request)
        }
    }

    private func scheduleRequestTimeout(_ request: Request, on eventLoop: any EventLoop) {
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

    private func schedulePingTimerForConnection(_ connectionID: PostgresConnection.ID, on eventLoop: any EventLoop) {
        self.backgroundLogger.trace("Schedule connection ping timer", metadata: [
            .connectionID: "\(connectionID)",
        ])
        let scheduled = eventLoop.scheduleTask(in: self.configuration.pingFrequency) {
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

    private func cancelPingTimerForConnection(_ connectionID: PostgresConnection.ID) {
        self.backgroundLogger.trace("Cancel connection ping timer", metadata: [
            .connectionID: "\(connectionID)",
        ])
        guard let cancelTimer = self._pingTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleIdleTimeoutTimerForConnection(_ connectionID: PostgresConnection.ID, on eventLoop: any EventLoop) {
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

    private func cancelIdleTimeoutTimerForConnection(_ connectionID: PostgresConnection.ID) {
        self.backgroundLogger.trace("Cancel idle connection timeout", metadata: [
            .connectionID: "\(connectionID)",
        ])
        guard let cancelTimer = self._idleTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have an idle timer for connection \(connectionID) at this point.")
        }
        cancelTimer.cancel()
    }

    private func scheduleConnectionStartBackoffTimer(
        _ connectionID: PostgresConnection.ID,
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

    private func cancelConnectionStartBackoffTimer(_ connectionID: PostgresConnection.ID) {
        guard let backoffTimer = self._backoffTimer.removeValue(forKey: connectionID) else {
            preconditionFailure("Expected to have a backoff timer for connection \(connectionID) at this point.")
        }
        backoffTimer.cancel()
    }

    private func closeConnection(_ connection: PostgresConnection) {
        self.backgroundLogger.debug("close connection", metadata: [
            PSQLConnection.LoggerMetaDataKey.connectionID.rawValue: "\(connection.id)",
        ])

        // we are not interested in the close promise... The connection will inform us about its
        // close anyway.
        connection.close(promise: nil)
    }
}

@available(macOS 13.0, iOS 16.0, *)
extension PostgresConnectionPool {
    struct Request: ConnectionRequest {
        private enum AsyncReportingMechanism {
            case continuation(CheckedContinuation<PostgresConnection, any Error>)
            case promise(EventLoopPromise<PostgresConnection>)
        }

        typealias ID = Int

        var id: ID

        var preferredEventLoop: NIOCore.EventLoop?

        var deadline: NIOCore.NIODeadline

        private var reportingMechanism: AsyncReportingMechanism

        init(
            id: Int,
            deadline: NIOCore.NIODeadline,
            continuation: CheckedContinuation<PostgresConnection, any Error>,
            preferredEventLoop: (any EventLoop)?
        ) {
            self.id = id
            self.deadline = deadline
            self.preferredEventLoop = preferredEventLoop
            self.reportingMechanism = .continuation(continuation)
        }

        fileprivate func succeed(_ connection: PostgresConnection) {
            switch self.reportingMechanism {
            case .continuation(let continuation):
                continuation.resume(returning: connection)
            case .promise(let promise):
                promise.succeed(connection)
            }
        }

        fileprivate func fail(_ error: any Error) {
            switch self.reportingMechanism {
            case .continuation(let continuation):
                continuation.resume(throwing: error)
            case .promise(let promise):
                promise.fail(error)
            }
        }
    }
}

extension PostgresConnection: PooledConnection {}

extension PostgresConnection.ID.Generator: ConnectionIDGeneratorProtocol {}

#endif
