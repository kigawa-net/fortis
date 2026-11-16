package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.logging.LogLevel
import net.kigawa.fortis.raft.logging.RaftLogEvents
import net.kigawa.fortis.raft.logging.RaftLogger
import net.kigawa.fortis.raft.logging.StructuredLog
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TxnCoordinatorLoggingTest {
    @Test
    fun recoveryAbortIsRecorded() = runTest {
        val logger = RecordingLogger()
        val coordinator = TxnCoordinator(emptyMap(), logger = logger)
        val txn = coordinator.begin()
        txn.record.state = TxnState.PREPARED

        assertEquals(1, coordinator.recover())
        assertEquals(TxnState.ABORTED, txn.record.state)
        assertEquals(
            listOf(
                StructuredLog(
                    LogLevel.WARN,
                    RaftLogEvents.COMPONENT_TXN,
                    RaftLogEvents.TXN_ABORTED,
                    mapOf("txnId" to txn.id.toString()),
                ),
            ),
            logger.entries,
        )
        assertEquals(0, coordinator.recover())
        assertEquals(1, logger.entries.size)
    }

    @Test
    fun commitAndAbortAreRecorded() = runTest {
        val logger = RecordingLogger()
        val engine = MemoryMvccStorageEngine()
        val coordinator = TxnCoordinator(
            mapOf("g1" to TxnParticipant("g1", engine)),
            logger = logger,
        )

        val txn = coordinator.begin()
        txn.put("g1", byteArrayOf(1), byteArrayOf(10))
        txn.commit()

        val committed = logger.entries.firstOrNull { it.event == RaftLogEvents.TXN_COMMITTED }
        assertNotNull(committed)
        assertEquals(LogLevel.INFO, committed.level)
        assertEquals(RaftLogEvents.COMPONENT_TXN, committed.component)
        assertEquals(txn.id.toString(), committed.fields["txnId"])

        val txn2 = coordinator.begin()
        txn2.put("g1", byteArrayOf(2), byteArrayOf(20))
        txn2.abort()

        val aborted = logger.entries.firstOrNull { it.event == RaftLogEvents.TXN_ABORTED }
        assertNotNull(aborted)
        assertEquals(LogLevel.WARN, aborted.level)
        assertEquals(RaftLogEvents.COMPONENT_TXN, aborted.component)
        assertEquals(txn2.id.toString(), aborted.fields["txnId"])
    }

    @Test
    fun nullLoggerRecordsNothing() = runTest {
        val engine = MemoryMvccStorageEngine()
        val coordinator = TxnCoordinator(mapOf("g1" to TxnParticipant("g1", engine)))

        val txn = coordinator.begin()
        txn.put("g1", byteArrayOf(1), byteArrayOf(10))
        txn.commit()
        txn.abort()
    }

    private class RecordingLogger : RaftLogger {
        val entries = mutableListOf<StructuredLog>()

        override fun log(
            level: LogLevel,
            component: String,
            event: String,
            fields: Map<String, String>,
        ) {
            entries.add(StructuredLog(level, component, event, fields))
        }
    }
}
