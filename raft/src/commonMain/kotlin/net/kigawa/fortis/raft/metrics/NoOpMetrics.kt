package net.kigawa.fortis.raft.metrics

/** 何も記録しない [FortisMetrics]（デフォルト無効化用）。 */
object NoOpMetrics : FortisMetrics {
    override suspend fun incrementCounter(name: String, delta: Long): Unit = Unit

    override suspend fun setGauge(name: String, value: Long): Unit = Unit

    override suspend fun snapshot(): MetricsSnapshot =
        MetricsSnapshot(emptyMap(), emptyMap())
}
