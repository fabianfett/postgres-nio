import PostgresNIO
import NIOCore
import Logging

@main
@available(macOS 14, *)
enum Server {
    static func main() async throws {
        var mlogger = Logger(label: "psql")
        mlogger.logLevel = .debug
        let logger = mlogger

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)

        var clientConfig = PostgresClient.Configuration()
        clientConfig.pool.minimumConnectionCount = 0
        clientConfig.pool.maximumConnectionSoftLimit = 8*4
        clientConfig.pool.maximumConnectionHardLimit = 12*4
        clientConfig.pool.keepAliveFrequency = .seconds(5)
        clientConfig.pool.connectionIdleTimeout = .seconds(15)

        clientConfig.server.host = "postgres-nio-tests.vsl"
        clientConfig.authentication.username = "postgres"
        clientConfig.authentication.database = "postgres"
        clientConfig.authentication.password = "password"

        mlogger.info("Lets go")

        let client = try PostgresClient(
            configuration: clientConfig,
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<2 {
                group.addTask {
                    do {
                        try await client.withConnection(logger: logger) { connection in
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

//        try await ContinuousClock().sleep(until: .now + .seconds(120))

//        try await client.shutdown(graceful: false)
    }
}
