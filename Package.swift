// swift-tools-version:5.10
import PackageDescription

let package = Package(
    name: "postgres-nio",
    platforms: [
        .macOS(.v14),
//        .iOS(.v13),
//        .watchOS(.v6),
//        .tvOS(.v13),
    ],
    products: [
        .library(name: "PostgresNIO", targets: ["PostgresNIO"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-collections.git", from: "1.0.4"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.59.0"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.19.0"),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", from: "2.25.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", "2.0.0" ..< "4.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.4.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.5.3"),
        .package(url: "https://github.com/swift-server/swift-service-lifecycle.git", from: "2.4.1"),

        .package(url: "https://github.com/ordo-one/package-benchmark.git", from: "1.23.4"),
    ],
    targets: [
        .target(
            name: "PostgresNIO",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "NIOTransportServices", package: "swift-nio-transport-services"),
                .product(name: "NIOTLS", package: "swift-nio"),
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
                .product(name: "NIOFoundationCompat", package: "swift-nio"),
                .product(name: "ServiceLifecycle", package: "swift-service-lifecycle"),
            ]
        ),
        .target(
            name: "_ConnectionPoolModule",
            dependencies: [
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "DequeModule", package: "swift-collections"),
            ],
            path: "Sources/ConnectionPoolModule"
        ),
        .target(
            name: "_ConnectionPoolTestUtils",
            dependencies: [
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "DequeModule", package: "swift-collections"),
                .target(name: "_ConnectionPoolModule"),
            ],
            path: "Sources/ConnectionPoolTestUtils"
        ),
        .testTarget(
            name: "PostgresNIOTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "ConnectionPoolModuleTests",
            dependencies: [
                .target(name: "_ConnectionPoolModule"),
                .target(name: "_ConnectionPoolTestUtils"),
                .product(name: "DequeModule", package: "swift-collections"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOConcurrencyHelpers", package: "swift-nio"),
                .product(name: "NIOEmbedded", package: "swift-nio"),
            ]
        ),
        .testTarget(
            name: "IntegrationTests",
            dependencies: [
                .target(name: "PostgresNIO"),
                .product(name: "NIOTestUtils", package: "swift-nio"),
            ]
        ),
        .executableTarget(
            name: "ConnectionPoolBenchmark",
            dependencies: [
                .product(name: "Benchmark", package: "package-benchmark"),
                .target(name: "_ConnectionPoolTestUtils"),
                .target(name: "_ConnectionPoolModule"),
            ],
            path: "Benchmarks/ConnectionPoolBenchmark",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark"),
            ]
        ),
        .executableTarget(
            name: "PostgresNIOBenchmark",
            dependencies: [
                .product(name: "Benchmark", package: "package-benchmark"),
            ],
            path: "Benchmarks/PostgresNIOBenchmark",
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "package-benchmark"),
            ]
        ),
    ]
)
