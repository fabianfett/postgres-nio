import PostgresNIO
import NIOCore
import Logging

@main
@available(macOS 13, *)
enum Server {
    static func main() async throws {
        var mlogger = Logger(label: "psql")
        mlogger.logLevel = .debug
        let logger = mlogger

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)

        var poolConfig = PostgresConnectionPoolConfiguration()
        poolConfig.minimumConnectionCount = 0
        poolConfig.maximumConnectionSoftLimit = 8*4
        poolConfig.maximumConnectionHardLimit = 12*4
        poolConfig.pingFrequency = .seconds(5)
        poolConfig.idleTimeout = .seconds(15)

        let connectionConfig = PostgresConnection.Configuration(
            connection: .init(host: "postgres-nio-tests.vs"),
            authentication: .init(username: "postgres", database: "postgres", password: "password"),
            tls: .disable
        )
        let factory = Factory(configuration: connectionConfig)

        let pool = PostgresConnectionPool(
            configuration: poolConfig,
            factory: factory,
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: mlogger
        )

//        try await ContinuousClock().sleep(until: .now + .seconds(10))

//        let rows = try await pool.query("SELECT 1", deadline: .now + .seconds(12), clock: .continuous, logger: mlogger)
//        for try await row in rows {
//            mlogger.info("Row received")
//        }

        await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<2 {
                group.addTask {
                    do {
                        try await pool.withConnection(logger: logger) { connection in
                            let rows = try await connection.query("SELECT 1", logger: logger)
                            for try await row in rows {
    //                            logger.info("Row received")
                            }
                        }
                    } catch {
                        logger.error("Error", metadata: ["error": "\(error)"])
                    }
                }
            }
        }

        try await ContinuousClock().sleep(until: .now + .seconds(120))

        try await pool.shutdown()
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
