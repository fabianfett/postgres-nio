import PostgresNIO
import NIOCore
import Logging

@main
@available(macOS 13, *)
enum Server {
    static func main() async throws {
        var logger = Logger(label: "psql")
        logger.logLevel = .debug

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        var poolConfig = PostgresConnectionPoolConfiguration()
        poolConfig.minimumConnectionCount = 4
        poolConfig.maximumConnectionSoftLimit = 8
        poolConfig.maximumConnectionHardLimit = 12

        let connectionConfig = PostgresConnection.Configuration(
            connection: .init(host: "127.0.0.1"),
            authentication: .init(username: "test_username", database: "test_database", password: "test_password"),
            tls: .disable
        )
        let factory = Factory(configuration: connectionConfig)

        let pool = PostgresConnectionPool(
            configuration: poolConfig,
            factory: factory,
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        try await ContinuousClock().sleep(until: .now + .seconds(10))

        let rows = try await pool.query("SELECT 1", deadline: .now + .seconds(12), clock: .continuous, logger: logger)
        for try await row in rows {
            logger.info("Row received")
        }

        try await pool.gracefulShutdown()
    }
}

struct Factory: PostgresConnectionFactory {

    let configuration: PostgresConnection.Configuration

    init(configuration: PostgresConnection.Configuration) {
        self.configuration = configuration
    }

    func makeConnection(
        on eventLoop: EventLoop,
        id: PostgresConnection.ID,
        backgroundLogger: Logger
    ) -> EventLoopFuture<PostgresConnection> {
        PostgresConnection.connect(
            on: eventLoop,
            configuration: self.configuration,
            id: id,
            logger: backgroundLogger
        )
    }
}
