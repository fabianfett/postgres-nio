import Benchmark

let benchmarks = {
    Benchmark(
        "Lease and release",
        configuration: Benchmark.Configuration(
            metrics: [
                .instructions,
                .wallClock,
                .cpuTotal,
                .contextSwitches,
                .mallocCountTotal,
            ],
            scalingFactor: .kilo,
            maxDuration: .seconds(3)
        ),
        closure: ConnectionPoolBenchmarks().leaseAndRelease(benchmark:)
    )

    Benchmark(
        "Lease and release 30x concurrently",
        configuration: Benchmark.Configuration(
            metrics: [
                .instructions,
                .wallClock,
                .cpuTotal,
                .contextSwitches,
                .mallocCountTotal,
            ],
            scalingFactor: .one,
            maxDuration: .seconds(10)
        ),
        closure: ConnectionPoolBenchmarks().leaseAndReleaseHighlyConcurrently(benchmark:)
    )

    Benchmark(
        "Lease and release 2000x concurrently",
        configuration: Benchmark.Configuration(
            metrics: [
                .wallClock,
            ],
            scalingFactor: .one,
            maxDuration: .seconds(60)
        ),
        closure: ConnectionPoolBenchmarks().leaseAndReleaseMegaHighlyConcurrently(benchmark:)
    )

}
