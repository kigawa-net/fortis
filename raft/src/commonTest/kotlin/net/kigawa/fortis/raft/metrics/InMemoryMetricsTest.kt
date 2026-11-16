package net.kigawa.fortis.raft.metrics

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InMemoryMetricsTest {
    @Test
    fun countersAccumulate() = runTest {
        val metrics = InMemoryMetrics()
        metrics.incrementCounter(RaftMetrics.APPLIED_ENTRIES)
        metrics.incrementCounter(RaftMetrics.APPLIED_ENTRIES, 2)

        assertEquals(3L, metrics.snapshot().counters[RaftMetrics.APPLIED_ENTRIES])
    }

    @Test
    fun gaugesOverwrite() = runTest {
        val metrics = InMemoryMetrics()
        metrics.setGauge(RaftMetrics.LAST_APPLIED, 5)
        metrics.setGauge(RaftMetrics.LAST_APPLIED, 9)

        assertEquals(9L, metrics.snapshot().gauges[RaftMetrics.LAST_APPLIED])
    }

    @Test
    fun snapshotIsDefensiveCopy() = runTest {
        val metrics = InMemoryMetrics()
        metrics.incrementCounter(RaftMetrics.TXN_COMMITTED)
        val first = metrics.snapshot()

        metrics.incrementCounter(RaftMetrics.TXN_COMMITTED)
        metrics.setGauge(RaftMetrics.COMMIT_INDEX, 7)

        assertEquals(1L, first.counters[RaftMetrics.TXN_COMMITTED])
        assertTrue(RaftMetrics.COMMIT_INDEX !in first.gauges)
        assertEquals(2L, metrics.snapshot().counters[RaftMetrics.TXN_COMMITTED])
    }

    @Test
    fun noOpMetricsRecordsNothing() = runTest {
        NoOpMetrics.incrementCounter(RaftMetrics.APPLIED_ENTRIES)
        NoOpMetrics.setGauge(RaftMetrics.LAST_APPLIED, 1)

        assertTrue(NoOpMetrics.snapshot().counters.isEmpty())
        assertTrue(NoOpMetrics.snapshot().gauges.isEmpty())
    }
}
