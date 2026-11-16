package net.kigawa.fortis.raft.metrics

/**
 * メトリクス取得結果の不変スナップショット。
 *
 * @property counters 累積カウンタ（加算のみ）。
 * @property gauges 現在値ゲージ（上書き）。
 */
data class MetricsSnapshot(
    val counters: Map<String, Long>,
    val gauges: Map<String, Long>,
)
