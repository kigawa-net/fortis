package net.kigawa.fortis.raft.metrics

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * メモリ保持の [FortisMetrics] 実装。
 *
 * [Mutex] で排他し、カウンタは加算・ゲージは上書きする。
 * [snapshot] は defensive copy を返す。
 */
class InMemoryMetrics : FortisMetrics {
    private val mutex = Mutex()
    private val counters = mutableMapOf<String, Long>()
    private val gauges = mutableMapOf<String, Long>()

    override suspend fun incrementCounter(name: String, delta: Long) {
        mutex.withLock {
            counters[name] = (counters[name] ?: 0L) + delta
        }
    }

    override suspend fun setGauge(name: String, value: Long) {
        mutex.withLock {
            gauges[name] = value
        }
    }

    override suspend fun snapshot(): MetricsSnapshot = mutex.withLock {
        MetricsSnapshot(counters.toMap(), gauges.toMap())
    }
}
