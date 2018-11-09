package sae.benchmark.tpch

import sae.benchmark.BenchmarkConfig

trait TPCHConfig extends BenchmarkConfig {

	override val debugMode: Boolean = false

	override val benchmarkGroup = "tpch"

	override val doWarmup: Boolean = true
	override val iterations: Int = 1

	override val mongoTransferRecords: Boolean = true
	override val mongoConnectionString: String = "mongodb://server.tpch.i3ql:27017"

}