//
//  File.swift
//  
//
//  Created by Fabian Fett on 22.05.24.
//

import _ConnectionPoolModule
import _ConnectionPoolTestUtils
import Benchmark

struct ConnectionPoolBenchmarks {

    func leaseAndRelease(benchmark: Benchmark) async throws {
        let factory = MockConnectionFactory<ContinuousClock>()

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = 1

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            await factory.nextConnectAttempt { _ in return 1 }

            benchmark.startMeasurement()

            for _ in benchmark.scaledIterations {
                try? await blackHole(pool.withConnection { _ in
                    return 1
                })
            }

            benchmark.stopMeasurement()

            taskGroup.cancelAll()

            factory.runningConnections.first?.closeIfClosing()
        }
    }

    func leaseAndReleaseHighlyConcurrently(benchmark: Benchmark) async throws {
        let factory = MockConnectionFactory<ContinuousClock>()

        let connectionCount = 30
        let leaseAndReleases = 1000

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = connectionCount
        config.maximumConnectionSoftLimit = connectionCount
        config.maximumConnectionHardLimit = connectionCount

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            for _ in 0..<connectionCount {
                await factory.nextConnectAttempt { _ in return 1 }
            }

            await withDiscardingTaskGroup { innerTaskGroup in
                benchmark.startMeasurement()

                for _ in 0..<leaseAndReleases {
                    innerTaskGroup.addTask {
                        try? await blackHole(pool.withConnection { _ in
                            try await Task.sleep(for: .milliseconds(2))
                            return 1
                        })
                    }
                }

                benchmark.stopMeasurement()
            }

            taskGroup.cancelAll()

            let allConnections = factory.runningConnections
            for connection in allConnections {
                try! await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

    func leaseAndReleaseMegaHighlyConcurrently(benchmark: Benchmark) async throws {
        let factory = MockConnectionFactory<ContinuousClock>()

        let connectionCount = 2000
        let leaseAndReleases = 20000

        var config = ConnectionPoolConfiguration()
        config.minimumConnectionCount = connectionCount
        config.maximumConnectionSoftLimit = connectionCount
        config.maximumConnectionHardLimit = connectionCount

        let pool = ConnectionPool(
            configuration: config,
            idGenerator: ConnectionIDGenerator(),
            requestType: ConnectionRequest<MockConnection>.self,
            keepAliveBehavior: MockPingPongBehavior(keepAliveFrequency: nil, connectionType: MockConnection.self),
            observabilityDelegate: NoOpConnectionPoolMetrics(connectionIDType: MockConnection.ID.self),
            clock: ContinuousClock()
        ) {
            try await factory.makeConnection(id: $0, for: $1)
        }

        await withTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                await pool.run()
            }

            for _ in 0..<connectionCount {
                await factory.nextConnectAttempt { _ in return 1 }
            }

            await withDiscardingTaskGroup { innerTaskGroup in
                benchmark.startMeasurement()

                for _ in 0..<leaseAndReleases {
                    innerTaskGroup.addTask {
                        try? await blackHole(pool.withConnection { _ in
                            await Task.yield()
                            return 1
                        })
                    }
                }

                benchmark.stopMeasurement()
            }

            taskGroup.cancelAll()

            let allConnections = factory.runningConnections
            for connection in allConnections {
                try! await connection.signalToClose
                connection.closeIfClosing()
            }
        }
    }

}
