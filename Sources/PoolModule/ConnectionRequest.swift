import NIOCore

@available(macOS 14, *)
public struct ConnectionRequest<Connection: PooledConnection>: ConnectionRequestProtocol {
    private enum AsyncReportingMechanism {
        case continuation(CheckedContinuation<Connection, Error>)
        case promise(EventLoopPromise<Connection>)
    }

    public typealias ID = Int

    public var id: ID

    public var deadline: NIOCore.NIODeadline?

    private var reportingMechanism: AsyncReportingMechanism

    init(
        id: Int,
        deadline: NIOCore.NIODeadline,
        continuation: CheckedContinuation<Connection, Error>
    ) {
        self.id = id
        self.deadline = deadline
        self.reportingMechanism = .continuation(continuation)
    }

    init(
        id: Int,
        deadline: NIOCore.NIODeadline,
        promise: EventLoopPromise<Connection>
    ) {
        self.id = id
        self.deadline = deadline
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

@available(macOS 14, *)
extension ConnectionPool where Request == ConnectionRequest<Connection> {
    public convenience init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator,
        factory: Factory,
        keepAliveBehavior: KeepAliveBehavior,
        metricsDelegate: MetricsDelegate,
        clock: Clock
    ) {
        self.init(
            configuration: configuration,
            idGenerator: idGenerator,
            factory: factory,
            requestType: ConnectionRequest<Connection>.self,
            keepAliveBehavior: keepAliveBehavior,
            metricsDelegate: metricsDelegate,
            clock: clock
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
                    continuation: continuation
                )

                self.leaseConnection(request)
            }
        } onCancel: {
            self.cancelConnectionRequest(requestID)
        }

        self.metricsDelegate.connectionLeased(id: connection.id)

        return connection
    }
}
