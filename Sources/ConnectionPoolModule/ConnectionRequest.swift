
public struct ConnectionRequest<Connection: PooledConnection>: ConnectionRequestProtocol {
    private enum AsyncReportingMechanism {
        case continuation(CheckedContinuation<Connection, Error>)
    }

    public typealias ID = Int

    public var id: ID

    private var reportingMechanism: AsyncReportingMechanism

    init(
        id: Int,
        continuation: CheckedContinuation<Connection, Error>
    ) {
        self.id = id
        self.reportingMechanism = .continuation(continuation)
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
        }
    }

    private func fail(_ error: Error) {
        switch self.reportingMechanism {
        case .continuation(let continuation):
            continuation.resume(throwing: error)
        }
    }
}

fileprivate let requestIDGenerator = ConnectionPoolModule.ConnectionIDGenerator()

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ConnectionPool where Request == ConnectionRequest<Connection> {
    public convenience init(
        configuration: ConnectionPoolConfiguration,
        idGenerator: ConnectionIDGenerator = ConnectionPoolModule.ConnectionIDGenerator(),
        keepAliveBehavior: KeepAliveBehavior,
        observabilityDelegate: ObservabilityDelegate,
        clock: Clock = ContinuousClock(),
        connectionFactory: @escaping ConnectionFactory
    ) {
        self.init(
            configuration: configuration,
            idGenerator: idGenerator,
            requestType: ConnectionRequest<Connection>.self,
            keepAliveBehavior: keepAliveBehavior,
            observabilityDelegate: observabilityDelegate,
            clock: clock,
            connectionFactory: connectionFactory
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
                    continuation: continuation
                )

                self.leaseConnection(request)
            }
        } onCancel: {
            self.cancelLeaseConnection(requestID)
        }

        return connection
    }

    public func withConnection<Result>(_ closure: (Connection) async throws -> Result) async throws -> Result {
        let connection = try await self.leaseConnection()
        defer { self.releaseConnection(connection) }
        return try await closure(connection)
    }
}
