import NIOCore
import NIOSSL
import Logging
import PoolModule

public final class PostgresClient: Sendable {

    public struct Configuration: Sendable {
        public struct Pool: Sendable {
            /// The minimum number of connections to preserve in the pool.
            ///
            /// If the pool is mostly idle and the Redis servers close these idle connections,
            /// the `RedisConnectionPool` will initiate new outbound connections proactively to avoid the number of available connections dropping below this number.
            public var minimumConnectionCount: Int = 0

            /// The maximum number of connections to for this pool, to be preserved.
            public var maximumConnectionSoftLimit: Int = 10

            public var maximumConnectionHardLimit: Int = 10

            public var maxConsecutivePicksFromEventLoopQueue: UInt8 = 16

            public var connectionIdleTimeout: TimeAmount = .seconds(60)

            public var keepAliveFrequency: TimeAmount = .seconds(30)

            public var keepAliveQuery: PostgresQuery = "SELECT 1;"

            public init() {}
        }

        public struct Authentication: Sendable {
            /// The username to connect with.
            ///
            /// - Default: postgres
            public var username: String = "postgres"

            /// The database to open on the server
            ///
            /// - Default: `nil`
            public var database: Optional<String> = "postgres"

            /// The database user's password.
            ///
            /// - Default: `nil`
            public var password: Optional<String> = "password"

            public init() {}
        }

        public struct TLS: Sendable {
            enum Base {
                case disable
                case prefer(NIOSSL.TLSConfiguration)
                case require(NIOSSL.TLSConfiguration)
            }

            var base: Base

            private init(_ base: Base) {
                self.base = base
            }

            /// Do not try to create a TLS connection to the server.
            public static var disable: Self = Self.init(.disable)

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, create an insecure connection.
            public static func prefer(_ sslContext: NIOSSL.TLSConfiguration) -> Self {
                self.init(.prefer(sslContext))
            }

            /// Try to create a TLS connection to the server. If the server supports TLS, create a TLS connection.
            /// If the server does not support TLS, fail the connection creation.
            public static func require(_ sslContext: NIOSSL.TLSConfiguration) -> Self {
                self.init(.require(sslContext))
            }
        }

        public struct Server: Sendable {
            /// The server to connect to
            ///
            /// - Default: localhost
            public var host: String = "localhost"

            /// The server port to connect to.
            ///
            /// - Default: 5432
            public var port: Int = 5432

            /// Require connection to provide `BackendKeyData`.
            /// For use with Amazon RDS Proxy, this must be set to false.
            ///
            /// - Default: true
            public var requireBackendKeyData: Bool = true

            /// Specifies a timeout to apply to a connection attempt.
            ///
            /// - Default: 10 seconds
            public var connectTimeout: TimeAmount = .seconds(10)
        }

        public var server = Server()
        public var authentication = Authentication()

        public var pool = Pool()
        public var tls = TLS.prefer(.makeClientConfiguration())

        public init() {}
    }

    typealias Pool = ConnectionPool<
        PostgresConnectionFactory,
        PostgresConnection,
        PostgresConnection.ID,
        ConnectionIDGenerator,
        ConnectionRequest<PostgresConnection>,
        ConnectionRequest.ID,
        PostgresKeepAliveBehavor,
        PostgresClientMetrics
    >

    let pool: Pool

    public init(configuration: Configuration, eventLoopGroup: EventLoopGroup, backgroundLogger: Logger) throws {
        self.pool = try ConnectionPool(
            configuration: .init(configuration),
            idGenerator: ConnectionIDGenerator(),
            factory: PostgresConnectionFactory(configuration: .init(configuration), logger: backgroundLogger),
            keepAliveBehavior: .init(configuration, logger: backgroundLogger),
            metricsDelegate: .init(logger: backgroundLogger),
            eventLoopGroup: eventLoopGroup
        )
    }

    @available(macOS 13.0, *)
    public func query<Clock: _Concurrency.Clock>(
        _ query: PostgresQuery,
        deadline: Clock.Instant,
        clock: Clock,
        logger: Logger,
        file: String = #file,
        line: Int = #line
    ) async throws -> PostgresRowSequence {
        let connection = try await self.pool.leaseConnection()

        return try await connection.query(query, logger: logger)
    }

    public func withConnection<Result>(logger: Logger, _ closure: (PostgresConnection) async throws -> Result) async throws -> Result {
        let connection = try await self.pool.leaseConnection()

        defer { self.pool.releaseConnection(connection) }

        return try await closure(connection)
    }

    public func withConnection<Result>(
        logger: Logger,
        preferredEventLoop: EventLoop,
        _ closure: @escaping @Sendable (PostgresConnection) -> EventLoopFuture<Result>
    ) -> EventLoopFuture<Result> {
        self.pool.withConnection(preferredEventLoop: preferredEventLoop, closure)
    }

    public func shutdown(graceful: Bool) async throws {
        try await self.pool.shutdown()
    }

    public func shutdown(graceful: Bool, promise: EventLoopPromise<Void>?) {
        self.pool.shutdown(promise: promise)
    }

    public func shutdown(graceful: Bool) -> EventLoopFuture<Void> {
        let promise = self.pool.eventLoopGroup.any().makePromise(of: Void.self)
        self.shutdown(graceful: graceful, promise: promise)
        return promise.futureResult
    }

    public func syncShutdown() throws {
        try self.shutdown(graceful: false).wait()
    }
}

struct PostgresConnectionFactory: ConnectionFactory {
    let configuration: PostgresConnection.Configuration
    let logger: Logger

    init(configuration: PostgresConnection.Configuration, logger: Logger) {
        self.configuration = configuration
        self.logger = logger
    }

    func makeConnection(
        on eventLoop: EventLoop,
        id: PostgresConnection.ID
    ) -> EventLoopFuture<PostgresConnection> {
        var connectionLogger = self.logger
        connectionLogger[postgresMetadataKey: .connectionID] = "\(id)"

        return PostgresConnection.connect(
            on: eventLoop,
            configuration: self.configuration,
            id: id,
            logger: connectionLogger
        )
    }
}

struct PostgresKeepAliveBehavor: ConnectionKeepAliveBehavior {
    var keepAliveFrequency: TimeAmount?
    var query: PostgresQuery
    var logger: Logger

    init(keepAliveFrequency: TimeAmount?, logger: Logger) {
        self.keepAliveFrequency = keepAliveFrequency
        self.query = "SELECT 1;"
        self.logger = logger
    }

    func runKeepAlive(for connection: PostgresConnection) -> EventLoopFuture<Void> {
        connection.query(self.query, logger: self.logger).map { _ in }
    }
}

extension PostgresKeepAliveBehavor {
    init(_ config: PostgresClient.Configuration, logger: Logger) {
        self = .init(keepAliveFrequency: config.pool.keepAliveFrequency, logger: logger)
        self.query = config.pool.keepAliveQuery
    }
}

extension ConnectionPoolConfiguration {
    init(_ config: PostgresClient.Configuration) {
        self = .init()
        self.minimumConnectionCount = config.pool.minimumConnectionCount
        self.maximumConnectionSoftLimit = config.pool.maximumConnectionSoftLimit
        self.maximumConnectionHardLimit = config.pool.maximumConnectionHardLimit
        self.idleTimeout = config.pool.connectionIdleTimeout
    }
}

extension PostgresConnection.Configuration {
    init(_ config: PostgresClient.Configuration) throws {
        try self.init(
            connection: .init(host: config.server.host, port: config.server.port),
            authentication: .init(
                username: config.authentication.username,
                database: config.authentication.database,
                password: config.authentication.password),
            tls: .init(config.tls)
        )
    }
}

extension PostgresConnection.Configuration.TLS {
    // TODO: Make async
    init(_ config: PostgresClient.Configuration.TLS) throws {
        switch config.base {
        case .disable:
            self = .disable
        case .prefer(let tlsConfig):
            self = try .prefer(.init(configuration: tlsConfig))
        case .require(let tlsConfig):
            self = try .require(.init(configuration: tlsConfig))
        }
    }
}

extension PostgresConnection: PooledConnection {
    public func close() {
        self.close(promise: nil)
    }

    public func onClose(_ closure: @escaping () -> ()) {
        self.closeFuture.whenComplete { _ in closure() }
    }
}

extension PoolError {
    func mapToPSQLError(lastConnectError: Error?) -> Error {
        let psqlError: any Error
        switch self {
        case .poolShutdown:
            psqlError = PSQLError.poolClosed
        case .requestTimeout:
            psqlError = lastConnectError ?? PSQLError.timeoutError
        case .requestCancelled:
            psqlError = PSQLError.queryCancelled
        default:
            return self
        }
        return psqlError
    }
}
