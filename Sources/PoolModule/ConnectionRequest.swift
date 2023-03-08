import NIOCore

public struct ConnectionRequest<Connection: PooledConnection>: ConnectionRequestProtocol {
    private enum AsyncReportingMechanism {
        case continuation(CheckedContinuation<Connection, Error>)
        case promise(EventLoopPromise<Connection>)
    }

    public typealias ID = Int

    public var id: ID

    public var preferredEventLoop: NIOCore.EventLoop?

    public var deadline: NIOCore.NIODeadline?

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

    public func complete(with result: Result<Connection, PoolError>) {
        switch result {
        case .success(let success):
            self.succeed(success)
        case .failure(let failure):
            self.fail(failure)
        }
    }

    private func succeed(_ connection: Connection) {
        switch self.reportingMechanism {
        case .continuation(let continuation):
            continuation.resume(returning: connection)
        case .promise(let promise):
            promise.succeed(connection)
        }
    }

    private func fail(_ error: Error) {
        switch self.reportingMechanism {
        case .continuation(let continuation):
            continuation.resume(throwing: error)
        case .promise(let promise):
            promise.fail(error)
        }
    }
}

fileprivate let requestIDGenerator = PoolModule.ConnectionIDGenerator()

extension ConnectionPool where Request == ConnectionRequest<Connection> {
    public convenience init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
        keepAliveBehavior: KeepAliveBehavior,
        metricsDelegate: MetricsDelegate,
        eventLoopGroup: EventLoopGroup
    ) {
        self.init(
            configuration: configuration,
            idGenerator: idGenerator,
            factory: factory,
            requestType: ConnectionRequest<Connection>.self,
            keepAliveBehavior: keepAliveBehavior,
            metricsDelegate: metricsDelegate,
            eventLoopGroup: eventLoopGroup
        )
    }

    public func leaseConnection() async throws -> Connection {
        let requestID = requestIDGenerator.next()

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

                self.leaseConnection(request)
            }
        } onCancel: {
            self.cancelConnectionRequest(requestID)
        }

        self.metricsDelegate.connectionLeased(id: connection.id)

        return connection
    }

    public func leaseConnection(preferredEventLoop: EventLoop) -> EventLoopFuture<Connection> {
        let requestID = requestIDGenerator.next()

        let promise = preferredEventLoop.makePromise(of: Connection.self)

        let request = Request(
            id: requestID,
            deadline: .now() + .seconds(10),
            promise: promise,
            preferredEventLoop: preferredEventLoop
        )

        self.leaseConnection(request)

        return promise.futureResult
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
}
