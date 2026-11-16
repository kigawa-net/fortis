package net.kigawa.fortis.raft.metrics

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.txn.TxnCoordinator
import net.kigawa.fortis.raft.txn.TxnParticipant
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** [RaftApplier] と [TxnCoordinator] の metrics フックを検証する。 */
class MetricsHookTest {
    @Test
    fun applierAdvancesAppliedEntries() = runTest {
        val volatileState = RaftVolatileState(commitIndex = 2)
        val log = MemoryRaftLog()
        log.append(RaftLogEntry(1, 1, RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))))
        log.append(RaftLogEntry(2, 1, RaftCommand.Put(byteArrayOf(2), byteArrayOf(20))))
        val metrics = InMemoryMetrics()

        RaftApplier(volatileState, log, RecordingStateMachine(), metrics).applyCommitted()

        val snapshot = metrics.snapshot()
        assertEquals(2L, snapshot.counters[RaftMetrics.APPLIED_ENTRIES])
        assertEquals(2L, snapshot.gauges[RaftMetrics.LAST_APPLIED])
    }

    @Test
    fun applierWithoutMetricsStillApplies() = runTest {
        val volatileState = RaftVolatileState(commitIndex = 1)
        val log = MemoryRaftLog()
        log.append(RaftLogEntry(1, 1, RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))))
        val stateMachine = RecordingStateMachine()

        // デフォルト引数（metrics なし）の既存呼び出し形が壊れないこと。
        RaftApplier(volatileState, log, stateMachine).applyCommitted()

        assertEquals(1, stateMachine.applied.size)
        assertEquals(1L, volatileState.lastApplied)
    }

    @Test
    fun coordinatorCountsCommitAndAbort() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val metrics = InMemoryMetrics()
        val coordinator = TxnCoordinator(
            mapOf("g1" to TxnParticipant("g1", engine)),
            metrics = metrics,
        )

        val committed = coordinator.begin()
        committed.put("g1", byteArrayOf(1), byteArrayOf(20))
        assertTrue(committed.commit())

        val aborted = coordinator.begin()
        aborted.get("g1", byteArrayOf(1))
        engine.putAt(byteArrayOf(1), byteArrayOf(30), 3)
        aborted.put("g1", byteArrayOf(1), byteArrayOf(40))
        assertFalse(aborted.commit())

        val snapshot = metrics.snapshot()
        assertEquals(1L, snapshot.counters[RaftMetrics.TXN_COMMITTED])
        assertEquals(1L, snapshot.counters[RaftMetrics.TXN_ABORTED])
    }

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
