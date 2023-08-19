
@available(macOS 14.0, *)
public protocol ConnectionKeepAliveBehavior: Sendable {
    associatedtype Connection: PooledConnection

    var keepAliveFrequency: Duration? { get }

    func runKeepAlive(for connection: Connection) async throws
}

@available(macOS 14.0, *)
public protocol ConnectionFactory {
    associatedtype Connection: PooledConnection
    associatedtype ConnectionID: Hashable where Connection.ID == ConnectionID
    associatedtype ConnectionIDGenerator: ConnectionIDGeneratorProtocol where ConnectionIDGenerator.ID == ConnectionID
    associatedtype Request: ConnectionRequestProtocol where Request.Connection == Connection
    associatedtype KeepAliveBehavior: ConnectionKeepAliveBehavior where KeepAliveBehavior.Connection == Connection
    associatedtype MetricsDelegate: ConnectionPoolMetricsDelegate where MetricsDelegate.ConnectionID == ConnectionID
    associatedtype Clock: _Concurrency.Clock where Clock.Duration == Duration

    func makeConnection(
        id: ConnectionID,
        for pool: ConnectionPool<Self, Connection, ConnectionID, ConnectionIDGenerator, Request, Request.ID, KeepAliveBehavior, MetricsDelegate, Clock>
    ) async throws -> ConnectionAndMetadata<Connection>
}

@available(macOS 14.0, *)
public struct ConnectionAndMetadata<Connection: PooledConnection> {

    public var connection: Connection

    public var maximalStreamsOnConnection: UInt16

    public init(connection: Connection, maximalStreamsOnConnection: UInt16) {
        self.connection = connection
        self.maximalStreamsOnConnection = maximalStreamsOnConnection
    }
}

@available(macOS 14.0, *)
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

    public var idleTimeout: Duration

    public init(coreCount: Int) {
        self.minimumConnectionCount = coreCount
        self.maximumConnectionSoftLimit = coreCount
        self.maximumConnectionHardLimit = coreCount * 4
        self.idleTimeout = .seconds(60)
    }
}

@available(macOS 14.0, *)
public protocol PooledConnection: AnyObject, Sendable {
    associatedtype ID: Hashable

    var id: ID { get }

    func onClose(_ closure: @escaping @Sendable ((any Error)?) -> ())

    func close()
}

@available(macOS 14.0, *)
public protocol ConnectionIDGeneratorProtocol {
    associatedtype ID: Hashable

    func next() -> ID
}

@available(macOS 14.0, *)
public protocol ConnectionRequestProtocol {
    associatedtype ID: Hashable
    associatedtype Connection: PooledConnection

    var id: ID { get }

    func complete(with: Result<Connection, PoolError>)
}

@available(macOS 14.0, *)
public final class ConnectionPool<
    Factory: ConnectionFactory,
    Connection: PooledConnection,
    ConnectionID: Hashable,
    ConnectionIDGenerator: ConnectionIDGeneratorProtocol,
    Request: ConnectionRequestProtocol,
    RequestID: Hashable,
    KeepAliveBehavior: ConnectionKeepAliveBehavior,
    MetricsDelegate: ConnectionPoolMetricsDelegate,
    Clock: _Concurrency.Clock
>: @unchecked Sendable where
    Factory.Connection == Connection,
    Factory.ConnectionID == ConnectionID,
    Factory.ConnectionIDGenerator == ConnectionIDGenerator,
    Factory.Request == Request,
    Factory.KeepAliveBehavior == KeepAliveBehavior,
    Factory.MetricsDelegate == MetricsDelegate,
    Factory.Clock == Clock,
    Connection.ID == ConnectionID,
    ConnectionIDGenerator.ID == ConnectionID,
    Request.Connection == Connection,
    Request.ID == RequestID,
    KeepAliveBehavior.Connection == Connection,
    MetricsDelegate.ConnectionID == ConnectionID,
    Clock.Duration == Duration
{
    @usableFromInline
    typealias StateMachine = PoolStateMachine<Connection, ConnectionIDGenerator, ConnectionID, Request, Request.ID>

    @usableFromInline
    let factory: Factory

    @usableFromInline
    let keepAliveBehavior: KeepAliveBehavior

    @usableFromInline 
    let metricsDelegate: MetricsDelegate

    @usableFromInline
    let clock: Clock

    @usableFromInline
    let configuration: ConnectionPoolConfiguration

    @usableFromInline let stateLock = NIOLock()
    @usableFromInline private(set) var _stateMachine: StateMachine
    @usableFromInline private(set) var _lastConnectError: Error?

    private let requestIDGenerator = ConnectionPoolModule.ConnectionIDGenerator()

    @usableFromInline
    let eventStream: AsyncStream<NewPoolActions>

    @usableFromInline
    let eventContinuation: AsyncStream<NewPoolActions>.Continuation

    public init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
        requestType: Request.Type,
        keepAliveBehavior: KeepAliveBehavior,
        metricsDelegate: MetricsDelegate,
        clock: Clock
    ) {
        self.clock = clock
        self.factory = factory
        self.keepAliveBehavior = keepAliveBehavior
        self.metricsDelegate = metricsDelegate
        self.configuration = configuration
        self._stateMachine = PoolStateMachine(
            configuration: .init(configuration, keepAliveBehavior: keepAliveBehavior),
            generator: idGenerator
        )

        let (stream, continuation) = AsyncStream.makeStream(of: NewPoolActions.self)
        self.eventStream = stream
        self.eventContinuation = continuation

        let connectionRequests = self._stateMachine.refillConnections()

        for request in connectionRequests {
            self.eventContinuation.yield(.makeConnection(request))
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
        let actions = self.stateLock.withLock { () -> [StateMachine.Action] in
            var actions = [StateMachine.Action]()
            actions.reserveCapacity(requests.count)

            for request in requests {
                let stateMachineAction = self._stateMachine.leaseConnection(request)
                actions.append(stateMachineAction)
            }

            return actions
        }

        for action in actions {
            self.runRequestAction(action.request)
            self.runConnectionAction(action.connection)
        }
    }

    public func cancelConnectionRequest(_ requestID: RequestID) {
        self.modifyStateAndRunActions { stateMachine in
            stateMachine.cancelRequest(id: requestID)
        }
    }

    /// Mark a connection as going away. Connection implementors have to call this method if the connection
    /// has received a close intent from the server. For example: an HTTP/2 GOWAY frame.
    public func connectionWillClose(_ connection: Connection) {

    }

    public func connectionDidClose(_ connection: Connection, error: (any Error)?) {
        self.metricsDelegate.connectionClosed(id: connection.id, error: error)

        self.modifyStateAndRunActions { stateMachine in
            stateMachine.connectionClosed(connection)
        }
    }

    public func connection(_ connection: Connection, didReceiveNewMaxStreamSetting: UInt16) {

    }

    public func run() async {
        await withTaskCancellationHandler {
            await withDiscardingTaskGroup() { taskGroup in
                await self.run(in: &taskGroup)
            }
        } onCancel: {
            let actions = self.stateLock.withLock {
                self._stateMachine.triggerForceShutdown()
            }

            self.runStateMachineActions(actions)
        }
    }

    // MARK: - Private Methods -

    // MARK: Events

    @usableFromInline
    enum NewPoolActions {
        case makeConnection(StateMachine.ConnectionRequest)
        case closeConnection(Connection)
        case runKeepAlive(Connection)

        case scheduleTimer(StateMachine.Timer)
    }

    private func run(in taskGroup: inout DiscardingTaskGroup) async {
        for await event in self.eventStream {
            switch event {
            case .makeConnection(let request):
                self.makeConnection(for: request, in: &taskGroup)

            case .runKeepAlive(let connection):
                self.runKeepAlive(connection, in: &taskGroup)

            case .closeConnection(let connection):
                self.closeConnection(connection)

            case .scheduleTimer(let timer):
                self.runTimer(timer, in: &taskGroup)
            }
        }
    }

    // MARK: Run actions

    @inlinable
    /*private*/ func modifyStateAndRunActions(_ closure: (inout StateMachine) -> StateMachine.Action) {
        let actions = self.stateLock.withLock { () -> StateMachine.Action in
            closure(&self._stateMachine)
        }
        self.runStateMachineActions(actions)
    }

    @inlinable
    /*private*/ func runStateMachineActions(_ actions: StateMachine.Action) {
        self.runConnectionAction(actions.connection)
        self.runRequestAction(actions.request)
    }

    @inlinable
    /*private*/ func runConnectionAction(_ action: StateMachine.ConnectionAction) {
        switch action {
        case .makeConnection(let request):
            self.eventContinuation.yield(.makeConnection(request))

        case .runKeepAlive(let connection, let cancelContinuation):
            cancelContinuation?.resume(returning: ())
            self.eventContinuation.yield(.runKeepAlive(connection))

        case .scheduleTimers(let timers):
            for timer in timers {
                self.eventContinuation.yield(.scheduleTimer(timer))
            }

        case .closeConnection(let connection):
            self.closeConnection(connection)

        case .shutdown(_):
            fatalError()

        case .none:
            break
        }
    }

    @inlinable
    /*private*/ func runRequestAction(_ action: StateMachine.RequestAction) {
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
    /*private*/ func makeConnection(for request: StateMachine.ConnectionRequest, in taskGroup: inout DiscardingTaskGroup) {
        taskGroup.addTask {
            self.metricsDelegate.startedConnecting(id: request.connectionID)

            do {
                let bundle = try await self.factory.makeConnection(id: request.connectionID, for: self)
                self.connectionEstablished(bundle)
                bundle.connection.onClose {
                    self.connectionDidClose(bundle.connection, error: $0)
                }
            } catch {
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
    /*private*/ func runKeepAlive(_ connection: Connection, in taskGroup: inout DiscardingTaskGroup) {
        self.metricsDelegate.keepAliveTriggered(id: connection.id)

        taskGroup.addTask {
            do {
                try await self.keepAliveBehavior.runKeepAlive(for: connection)

                self.metricsDelegate.keepAliveSucceeded(id: connection.id)

                self.modifyStateAndRunActions { stateMachine in
                    stateMachine.connectionKeepAliveDone(connection)
                }
            } catch {
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

    @usableFromInline
    enum TimerRunResult {
        case timerTriggered
        case timerCancelled
        case cancellationContinuationFinished
    }

    @inlinable
    /*private*/ func runTimer(_ timer: StateMachine.Timer, in poolGroup: inout DiscardingTaskGroup) {
        poolGroup.addTask(priority: nil) { () async -> () in
            await withTaskGroup(of: TimerRunResult.self, returning: Void.self) { taskGroup in
                taskGroup.addTask {
                    do {
                        try await self.clock.sleep(for: timer.duration)
                        return .timerTriggered
                    } catch {
                        return .timerCancelled
                    }
                }

                taskGroup.addTask {
                    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                        let continuation = self.stateLock.withLock {
                            self._stateMachine.timerScheduled(timer, cancelContinuation: continuation)
                        }

                        continuation?.resume(returning: ())
                    }

                    return .cancellationContinuationFinished
                }

                switch await taskGroup.next()! {
                case .cancellationContinuationFinished:
                    taskGroup.cancelAll()
                case .timerCancelled:
                    fatalError()
                case .timerTriggered:
                    let action = self.stateLock.withLock {
                        self._stateMachine.timerTriggered(timer)
                    }

                    self.runStateMachineActions(action)
                }

                return
            }
        }
    }
}

@available(macOS 14.0, *)
extension PoolConfiguration {
    init<KeepAliveBehavior: ConnectionKeepAliveBehavior>(_ configuration: ConnectionPoolConfiguration, keepAliveBehavior: KeepAliveBehavior) {
        self.minimumConnectionCount = configuration.minimumConnectionCount
        self.maximumConnectionSoftLimit = configuration.maximumConnectionSoftLimit
        self.maximumConnectionHardLimit = configuration.maximumConnectionHardLimit
        self.keepAliveDuration = keepAliveBehavior.keepAliveFrequency
        self.idleTimeoutDuration = configuration.idleTimeout
    }
}
