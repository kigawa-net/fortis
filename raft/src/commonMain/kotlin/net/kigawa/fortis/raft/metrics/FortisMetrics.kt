package net.kigawa.fortis.raft.metrics

/**
 * Fortis の observability 基盤。
 *
 * カウンタは単調増加の [Long]、ゲージは最新値の [Long] で表す。
 * 時刻取得 API を commonMain で使えないため、時刻が必要な値は
 * 呼び出し側注入または版番号で代用する。
 */
interface FortisMetrics {
    /** 指定名のカウンタを [delta] だけ加算する。 */
    suspend fun incrementCounter(name: String, delta: Long = 1)

    /** 指定名のゲージを [value] で上書きする。 */
    suspend fun setGauge(name: String, value: Long)

    /** 現在値の不変コピーを返す。 */
    suspend fun snapshot(): MetricsSnapshot
}
