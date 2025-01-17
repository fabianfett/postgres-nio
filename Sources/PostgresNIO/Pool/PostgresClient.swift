import NIOCore
import NIOSSL
import Atomics
import Logging
import _ConnectionPoolModule

/// A Postgres client that is backed by an underlying connection pool. Use ``Configuration`` to change the client's
/// behavior.
///
/// > Important:
/// The client can only lease connections if the user is running the client's ``run()`` method in a long running task:
///
/// ```swift
/// let client = PostgresClient(configuration: configuration, logger: logger)
/// await withTaskGroup(of: Void.self) {
///   taskGroup.addTask {
///     client.run() // !important
///   }
///
///   taskGroup.addTask {
///     client.withConnection { connection in
///       do {
///         let rows = try await connection.query("SELECT userID, name, age FROM users;")
///         for try await (userID, name, age) in rows.decode((UUID, String, Int).self) {
///           // do something with the values
///         }
///       } catch {
///         // handle errors
///       }
///     }
///   }
/// }
/// ```
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
@_spi(ConnectionPool)
public final class PostgresClient: Sendable {
    public struct Configuration: Sendable {
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

        // MARK: Client options

        /// Describes general client behavior options. Those settings are considered advanced options.
        public struct Options: Sendable {
            /// A keep-alive behavior for Postgres connections. The ``frequency`` defines after which time an idle
            /// connection shall run a keep-alive ``query``.
            public struct KeepAliveBehavior: Sendable {
                /// The amount of time that shall pass before an idle connection runs a keep-alive ``query``.
                public var frequency: Duration

                /// The ``query`` that is run on an idle connection after it has been idle for ``frequency``.
                public var query: PostgresQuery

                /// Create a new `KeepAliveBehavior`.
                /// - Parameters:
                ///   - frequency: The amount of time that shall pass before an idle connection runs a keep-alive `query`.
                ///                Defaults to `30` seconds.
                ///   - query: The `query` that is run on an idle connection after it has been idle for `frequency`.
                ///            Defaults to `SELECT 1;`.
                public init(frequency: Duration = .seconds(30), query: PostgresQuery = "SELECT 1;") {
                    self.frequency = frequency
                    self.query = query
                }
            }

            /// A timeout for creating a TCP/Unix domain socket connection. Defaults to `10` seconds.
            public var connectTimeout: Duration = .seconds(10)

            /// The server name to use for certificate validation and SNI (Server Name Indication) when TLS is enabled.
            /// Defaults to none (but see below).
            ///
            /// > When set to `nil`:
            /// If the connection is made to a server over TCP using
            /// ``PostgresConnection/Configuration/init(host:port:username:password:database:tls:)``, the given `host`
            /// is used, unless it was an IP address string. If it _was_ an IP, or the connection is made by any other
            /// method, SNI is disabled.
            public var tlsServerName: String? = nil

            /// Whether the connection is required to provide backend key data (internal Postgres stuff).
            ///
            /// This property is provided for compatibility with Amazon RDS Proxy, which requires it to be `false`.
            /// If you are not using Amazon RDS Proxy, you should leave this set to `true` (the default).
            public var requireBackendKeyData: Bool = true

            /// The minimum number of connections that the client shall keep open at any time, even if there is no
            /// demand. Default to `0`.
            ///
            /// If the open connection count becomes less than ``minimumConnections`` new connections
            /// are created immidiatly. Must be greater or equal to zero and less than ``maximumConnections``.
            ///
            /// Idle connections are kept alive using the ``keepAliveBehavior``.
            public var minimumConnections: Int = 0

            /// The maximum number of connections that the client may open to the server at any time. Must be greater
            /// than ``minimumConnections``. Defaults to `20` connections.
            ///
            /// Connections, that are created in response to demand are kept alive for the ``connectionIdleTimeout``
            /// before they are dropped.
            public var maximumConnections: Int = 20

            /// The maximum amount time that a connection that is not part of the ``minimumConnections`` is kept
            /// open without being leased. Defaults to `60` seconds.
            public var connectionIdleTimeout: Duration = .seconds(60)

            /// The ``KeepAliveBehavior-swift.struct`` to ensure that the underlying tcp-connection is still active
            /// for idle connections. `Nil` means that the client shall not run keep alive queries to the server. Defaults to a
            /// keep alive query of `SELECT 1;` every `30` seconds.
            public var keepAliveBehavior: KeepAliveBehavior? = KeepAliveBehavior()

            /// Create an options structure with default values.
            ///
            /// Most users should not need to adjust the defaults.
            public init() {}
        }

        // MARK: - Accessors

        /// The hostname to connect to for TCP configurations.
        ///
        /// Always `nil` for other configurations.
        public var host: String? {
            if case let .connectTCP(host, _) = self.endpointInfo { return host }
            else { return nil }
        }

        /// The port to connect to for TCP configurations.
        ///
        /// Always `nil` for other configurations.
        public var port: Int? {
            if case let .connectTCP(_, port) = self.endpointInfo { return port }
            else { return nil }
        }

        /// The socket path to connect to for Unix domain socket connections.
        ///
        /// Always `nil` for other configurations.
        public var unixSocketPath: String? {
            if case let .bindUnixDomainSocket(path) = self.endpointInfo { return path }
            else { return nil }
        }

        /// The TLS mode to use for the connection. Valid for all configurations.
        ///
        /// See ``TLS-swift.struct``.
        public var tls: TLS = .prefer(.makeClientConfiguration())

        /// Options for handling the communication channel. Most users don't need to change these.
        ///
        /// See ``Options-swift.struct``.
        public var options: Options = .init()

        /// The username to connect with.
        public var username: String

        /// The password, if any, for the user specified by ``username``.
        ///
        /// - Warning: `nil` means "no password provided", whereas `""` (the empty string) is a password of zero
        ///   length; these are not the same thing.
        public var password: String?

        /// The name of the database to open.
        ///
        /// - Note: If set to `nil` or an empty string, the provided ``username`` is used.
        public var database: String?

        // MARK: - Initializers

        /// Create a configuration for connecting to a server with a hostname and optional port.
        ///
        /// This specifies a TCP connection. If you're unsure which kind of connection you want, you almost
        /// definitely want this one.
        ///
        /// - Parameters:
        ///   - host: The hostname to connect to.
        ///   - port: The TCP port to connect to (defaults to 5432).
        ///   - tls: The TLS mode to use.
        public init(host: String, port: Int = 5432, username: String, password: String?, database: String?, tls: TLS) {
            self.init(endpointInfo: .connectTCP(host: host, port: port), tls: tls, username: username, password: password, database: database)
        }

        /// Create a configuration for connecting to a server through a UNIX domain socket.
        ///
        /// - Parameters:
        ///   - path: The filesystem path of the socket to connect to.
        ///   - tls: The TLS mode to use. Defaults to ``TLS-swift.struct/disable``.
        public init(unixSocketPath: String, username: String, password: String?, database: String?) {
            self.init(endpointInfo: .bindUnixDomainSocket(path: unixSocketPath), tls: .disable, username: username, password: password, database: database)
        }

        // MARK: - Implementation details

        enum EndpointInfo {
            case bindUnixDomainSocket(path: String)
            case connectTCP(host: String, port: Int)
        }

        var endpointInfo: EndpointInfo

        init(endpointInfo: EndpointInfo, tls: TLS, username: String, password: String?, database: String?) {
            self.endpointInfo = endpointInfo
            self.tls = tls
            self.username = username
            self.password = password
            self.database = database
        }
    }

    typealias Pool = ConnectionPool<
        PostgresConnection,
        PostgresConnection.ID,
        ConnectionIDGenerator,
        ConnectionRequest<PostgresConnection>,
        ConnectionRequest.ID,
        PostgresKeepAliveBehavor,
        PostgresClientMetrics,
        ContinuousClock
    >

    let pool: Pool
    let factory: ConnectionFactory
    let runningAtomic = ManagedAtomic(false)
    let backgroundLogger: Logger
    
    /// Creates a new ``PostgresClient``. Don't forget to run ``run()`` the client in a long running task.
    /// - Parameters:
    ///   - configuration: The client's configuration. See ``Configuration`` for details.
    ///   - eventLoopGroup: The underlying NIO `EventLoopGroup`. Defaults to ``defaultEventLoopGroup``.
    ///   - backgroundLogger: A `swift-log` `Logger` to log background messages to. A copy of this logger is also
    ///                       forwarded to the created connections as a background logger.
    public init(
        configuration: Configuration,
        eventLoopGroup: any EventLoopGroup = PostgresClient.defaultEventLoopGroup,
        backgroundLogger: Logger
    ) {
        let factory = ConnectionFactory(config: configuration, eventLoopGroup: eventLoopGroup, logger: backgroundLogger)
        self.factory = factory
        self.backgroundLogger = backgroundLogger

        self.pool = ConnectionPool(
            configuration: .init(configuration),
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<PostgresConnection>.self,
            keepAliveBehavior: .init(configuration.options.keepAliveBehavior, logger: backgroundLogger),
            observabilityDelegate: .init(logger: backgroundLogger),
            clock: ContinuousClock()
        ) { (connectionID, pool) in
            let connection = try await factory.makeConnection(connectionID, pool: pool)

            return ConnectionAndMetadata(connection: connection, maximalStreamsOnConnection: 1)
        }
    }

    
    /// Lease a connection for the provided `closure`'s lifetime.
    ///
    /// - Parameter closure: A closure that uses the passed `PostgresConnection`. The closure **must not** capture
    ///                      the provided `PostgresConnection`.
    /// - Returns: The closure's return value.
    public func withConnection<Result>(_ closure: (PostgresConnection) async throws -> Result) async throws -> Result {
        let connection = try await self.leaseConnection()

        defer { self.pool.releaseConnection(connection) }

        return try await closure(connection)
    }

    /// The client's run method. Users must call this function in order to start the client's background task processing
    /// like creating and destroying connections and running timers. 
    ///
    /// Calls to ``withConnection(_:)`` will emit a `logger` warning, if ``run()`` hasn't been called previously.
    public func run() async {
        let atomicOp = self.runningAtomic.compareExchange(expected: false, desired: true, ordering: .relaxed)
        precondition(!atomicOp.original, "PostgresClient.run() should just be called once!")
        await self.pool.run()
    }

    // MARK: - Private Methods -

    private func leaseConnection() async throws -> PostgresConnection {
        if !self.runningAtomic.load(ordering: .relaxed) {
            self.backgroundLogger.warning("Trying to lease connection from `PostgresClient`, but `PostgresClient.run()` hasn't been called yet.")
        }
        return try await self.pool.leaseConnection()
    }

    /// Returns the default `EventLoopGroup` singleton, automatically selecting the best for the platform.
    ///
    /// This will select the concrete `EventLoopGroup` depending which platform this is running on.
    public static var defaultEventLoopGroup: EventLoopGroup {
        PostgresConnection.defaultEventLoopGroup
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PostgresKeepAliveBehavor: ConnectionKeepAliveBehavior {
    let behavior: PostgresClient.Configuration.Options.KeepAliveBehavior?
    let logger: Logger

    init(_ behavior: PostgresClient.Configuration.Options.KeepAliveBehavior?, logger: Logger) {
        self.behavior = behavior
        self.logger = logger
    }

    var keepAliveFrequency: Duration? {
        self.behavior?.frequency
    }

    func runKeepAlive(for connection: PostgresConnection) async throws {
        try await connection.query(self.behavior!.query, logger: self.logger).map { _ in }.get()
    }
}

@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
extension ConnectionPoolConfiguration {
    init(_ config: PostgresClient.Configuration) {
        self = ConnectionPoolConfiguration()
        self.minimumConnectionCount = config.options.minimumConnections
        self.maximumConnectionSoftLimit = config.options.maximumConnections
        self.maximumConnectionHardLimit = config.options.maximumConnections
        self.idleTimeout = config.options.connectionIdleTimeout
    }
}

@_spi(ConnectionPool)
extension PostgresConnection: PooledConnection {
    public func close() {
        self.channel.close(mode: .all, promise: nil)
    }

    public func onClose(_ closure: @escaping ((any Error)?) -> ()) {
        self.closeFuture.whenComplete { _ in closure(nil) }
    }
}

extension ConnectionPoolError {
    func mapToPSQLError(lastConnectError: Error?) -> Error {
        var psqlError: PSQLError
        switch self {
        case .poolShutdown:
            psqlError = PSQLError.poolClosed
            psqlError.underlying = self

        case .requestCancelled:
            psqlError = PSQLError.queryCancelled
            psqlError.underlying = self

        default:
            return self
        }
        return psqlError
    }
}
