package net.kigawa.fortis.raft.backup

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class RaftBackupDrillTest {
    private val codec = RaftBackupCodec()

    @Test
    fun backupRestoreDrillPreservesStateAndLog() = runTest {
        val log = logWithEntries(3)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(3, "node-2"))
        val metadata = SnapshotMetadata(2, 2)

        val backup = RaftBackupProcedures.backup(log, store, upToIndex = 3, metadata = metadata)
        val restored = codec.import(codec.export(backup))
        assertEquals(backup, restored)

        val targetLog = MemoryRaftLog()
        val targetStore = MemoryRaftPersistentStateStore()
        RaftBackupProcedures.restore(restored, targetLog, targetStore)

        assertEquals(RaftPersistentState(3, "node-2"), targetStore.load())
        assertEquals(3L, targetLog.lastIndex())
        (1L..3L).forEach { index ->
            assertEquals(log.get(index), targetLog.get(index))
        }

        val next = RaftLogEntry(
            index = 4,
            term = 3,
            command = RaftCommand.Put(byteArrayOf(9), byteArrayOf(9)),
        )
        targetLog.append(next)
        assertEquals(next, targetLog.get(4))
    }

    @Test
    fun partialAndEmptyBackup() = runTest {
        val log = logWithEntries(3)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(2, null))

        val partial = RaftBackupProcedures.backup(log, store, upToIndex = 2)
        assertEquals(2, partial.entries.size)
        assertNull(partial.metadata)
        assertEquals(partial, codec.import(codec.export(partial)))

        val empty = RaftBackupProcedures.backup(log, store, upToIndex = 0)
        assertEquals(0, empty.entries.size)
        val targetLog = MemoryRaftLog()
        val targetStore = MemoryRaftPersistentStateStore()
        RaftBackupProcedures.restore(codec.import(codec.export(empty)), targetLog, targetStore)
        assertEquals(RaftPersistentState(2, null), targetStore.load())
        assertEquals(0L, targetLog.lastIndex())
    }

    @Test
    fun corruptedBackupFailsClosed() = runTest {
        val log = logWithEntries(2)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(1, "node-1"))
        val exported = codec.export(RaftBackupProcedures.backup(log, store, upToIndex = 2))

        val badMagic = exported.copyOf().also { it[0] = 0 }
        assertFailsWith<RaftBackupCorruptionException> { codec.import(badMagic) }

        val truncated = exported.copyOf(exported.size - 1)
        assertFailsWith<RaftBackupCorruptionException> { codec.import(truncated) }

        val trailed = exported + byteArrayOf(0)
        assertFailsWith<RaftBackupCorruptionException> { codec.import(trailed) }
    }

    @Test
    fun restoreOntoNonEmptyLogIsRejected() = runTest {
        val log = logWithEntries(1)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(1, null))
        val backup = RaftBackupProcedures.backup(log, store, upToIndex = 1)

        val targetLog = logWithEntries(1)
        assertFailsWith<IllegalArgumentException> {
            RaftBackupProcedures.restore(backup, targetLog, MemoryRaftPersistentStateStore())
        }
    }

    @Test
    fun backupBeyondLastIndexIsRejected() = runTest {
        val log = logWithEntries(1)
        val store = MemoryRaftPersistentStateStore()
        assertFailsWith<IllegalArgumentException> {
            RaftBackupProcedures.backup(log, store, upToIndex = 2)
        }
    }

    private suspend fun logWithEntries(count: Long): MemoryRaftLog {
        val log = MemoryRaftLog()
        for (index in 1L..count) {
            log.append(entry(index))
        }
        return log
    }

    private fun entry(index: Long): RaftLogEntry = when ((index % 3).toInt()) {
        1 -> RaftLogEntry(
            index = index,
            term = 2,
            command = RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(10)),
        )

        2 -> RaftLogEntry(
            index = index,
            term = 2,
            command = RaftCommand.Delete(byteArrayOf(index.toByte())),
        )

        else -> RaftLogEntry(index = index, term = 3, payload = RaftLogEntryPayload.NoOp)
    }
}
