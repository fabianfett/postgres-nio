import PostgresNIO
import NIOCore
import Logging

@main
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
enum Server {
    static func main() async throws {
        var mlogger = Logger(label: "psql")
        mlogger.logLevel = .info
        let logger = mlogger

        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)

        var clientConfig = PostgresClient.Configuration()
        clientConfig.pool.minimumConnectionCount = 0
        clientConfig.pool.maximumConnectionSoftLimit = 8*4
        clientConfig.pool.maximumConnectionHardLimit = 12*4
        clientConfig.pool.keepAliveFrequency = .seconds(5)
        clientConfig.pool.connectionIdleTimeout = .seconds(15)

        clientConfig.server.host = "localhost"
        clientConfig.authentication.username = "test_username"
        clientConfig.authentication.database = "test_database"
        clientConfig.authentication.password = "test_password"

        let client = try PostgresClient(
            configuration: clientConfig,
            eventLoopGroup: eventLoopGroup,
            backgroundLogger: logger
        )

        let iterations = 1000

        await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                logger.info("Lets go")
                await client.run()
            }

            for i in 0..<iterations {
                group.addTask {
                    do {
                        try await client.withConnection(logger: logger) { connection in
                            let rows = try await connection.query("SELECT \(i)", logger: logger)
                            for try await row in rows.decode(Int.self) {
                                logger.info("Row received: \(row)")
                            }
                        }
                    } catch {
                        logger.error("Error", metadata: ["error": "\(error)"])
                    }
                }
            }

            var counter = 0
            while let _ = await group.nextResult() {
                counter += 1

                if counter == iterations {
                    group.cancelAll()
                }
            }
        }
    }
}
