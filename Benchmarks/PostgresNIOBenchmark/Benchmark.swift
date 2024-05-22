import Benchmarks

let benchmarks = {
    Benchmark("Minimal benchmark") { benchmark in
        // measure something here
    }

    Benchmark(
        "All metrics, full concurrency, async",
        configuration: .init(
            metrics: BenchmarkMetric.all,
            maxDuration: .seconds(10)
        )
    ) { benchmark in
        let _ = await withTaskGroup(of: Void.self, returning: Void.self, body: { taskGroup in
            for _ in 0..<80  {
                taskGroup.addTask {
                    
                }
            }
            for await _ in taskGroup {
            }
        }
    }
}
