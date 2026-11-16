package net.kigawa.fortis.raft.snapshot

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.io.fs.*
import net.kigawa.fortis.raft.*
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.log.*
import net.kigawa.fortis.raft.node.*
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.*

class SnapshotDurabilityTest {
    private val boundary = SnapshotMetadata(3, 2)
    private fun Path.file() = Fs.getPath(toAbsolutePath().toString()).toFile()
    private fun entry(i: Long, term: Long = 2) = RaftLogEntry(i, term,
        RaftCommand.Put(byteArrayOf(i.toByte()), byteArrayOf((i * 10).toByte())))

    @Test
    fun fileCompactionPhysicallyRemovesPrefixAndReopensSuffix() = runTest {
        directory { dir ->
            val path = dir.resolve("log")
            val log = FileRaftLog.open(path.file())
            for (i in 1L..5L) log.append(entry(i))
            val oldSize = Files.size(path)
            assertFailsWith<IllegalArgumentException> { log.compactThrough(boundary, 2) }
            log.compactThrough(boundary, 3)
            assertTrue(Files.size(path) < oldSize)
            val reopened = FileRaftLog.open(path.file())
            assertEquals(boundary, reopened.snapshotMetadata())
            assertEquals(5, reopened.lastIndex())
            assertNull(reopened.get(3))
            assertEquals(entry(4), reopened.get(4))
            reopened.truncateFrom(4)
            assertEquals(3, reopened.lastIndex())
            reopened.append(entry(4))
            assertEquals(entry(4), FileRaftLog.open(path.file()).get(4))
        }
    }

    @Test
    fun mismatchingFileBoundaryDiscardsSuffixAndRetainsNumbering() = runTest {
        directory { dir ->
            val file = dir.resolve("log").file()
            val log = FileRaftLog.open(file)
            for (i in 1L..5L) log.append(entry(i))
            val metadata = SnapshotMetadata(3, 4)
            log.compactThrough(metadata, 3)
            val reopened = FileRaftLog.open(file)
            assertEquals(metadata, reopened.snapshotMetadata())
            assertEquals(3, reopened.lastIndex())
            assertNull(reopened.get(4))
            reopened.append(entry(4, 4))
            assertEquals(entry(4, 4), FileRaftLog.open(file).get(4))
        }
    }

    @Test
    fun savedSnapshotRestoresDeletedMemoryAndCommittedSuffixAfterRestart() = runTest {
        directory { dir ->
            val storage = MemoryMvccStorageEngine()
            val machine = StorageEngineStateMachine(storage)
            val stateStore = FileRaftPersistentStateStore(dir.resolve("state").file())
            stateStore.save(2, null)
            val logFile = dir.resolve("log").file()
            val snapshotFile = dir.resolve("snapshot").file()
            val log = FileRaftLog.open(logFile)
            for (i in 1L..3L) {
                log.append(entry(i))
                machine.applyAt(entry(i).command!!, i)
            }
            val state = RaftVolatileState(commitIndex = 3, lastApplied = 3)
            val node = RaftNodeBuilder("f", setOf("l"), stateStore, state, log, machine).build()
            val runtime = RaftRuntime(node, LocalRaftTransport(), snapshotStore = FileRaftSnapshotStore(snapshotFile))
            runtime.saveSnapshot(boundary, machine.snapshotData())
            log.append(entry(4))
            // メモリ上の値を削除しても永続スナップショットは残る。
            storage.delete(byteArrayOf(1))
            assertNull(machine.get(byteArrayOf(1)))
            runtime.stop()

            val restored = StorageEngineStateMachine(MemoryMvccStorageEngine())
            val restoredState = RaftVolatileState()
            val reopened = RaftNodeBuilder("f", setOf("l"),
                FileRaftPersistentStateStore(dir.resolve("state").file()), restoredState,
                FileRaftLog.open(logFile), restored).build(FileRaftSnapshotStore.open(snapshotFile),
                RaftSnapshotApplier { _, data -> restored.restoreSnapshot(data) })
            assertContentEquals(byteArrayOf(10), restored.get(byteArrayOf(1)))
            assertEquals(3, restoredState.lastApplied)
            assertNull(restored.get(byteArrayOf(4)))
            assertTrue(reopened.handleAppendEntries(AppendEntriesRequest(2, "l", 3, 2, listOf(entry(4)), 4)).value.success)
            assertContentEquals(byteArrayOf(40), restored.get(byteArrayOf(4)))
            assertEquals(4, restoredState.lastApplied)
        }
    }

    @Test
    fun restartBetweenSnapshotPublicationAndCompactionCompletesCompaction() = runTest {
        directory { dir ->
            val logFile = dir.resolve("log").file()
            val log = FileRaftLog.open(logFile)
            for (i in 1L..5L) log.append(entry(i))
            val store = FileRaftSnapshotStore(dir.resolve("snapshot").file())
            store.save(boundary, byteArrayOf(9))
            val recoveredLog = FileRaftLog.open(logFile)
            val state = RaftVolatileState()
            var restored = false
            RaftSnapshotRecovery.restore(store, recoveredLog, state, RaftSnapshotApplier { _, data ->
                assertContentEquals(byteArrayOf(9), data)
                restored = true
            })
            assertTrue(restored)
            assertEquals(3, state.commitIndex)
            assertNull(recoveredLog.get(3))
            assertEquals(entry(4), recoveredLog.get(4))
        }
    }

    @Test
    fun restartRejectsMissingOrOlderSnapshot() = runTest {
        directory { dir ->
            val log = FileRaftLog.open(dir.resolve("log").file())
            log.compactThrough(boundary, 3)
            val store = FileRaftSnapshotStore(dir.resolve("snapshot").file())
            assertFailsWith<RaftSnapshotCorruptionException> {
                RaftSnapshotRecovery.restore(store, log, RaftVolatileState(), RaftSnapshotApplier.NoOp)
            }
            store.save(SnapshotMetadata(2, 2), byteArrayOf(1))
            assertFailsWith<RaftSnapshotCorruptionException> {
                RaftSnapshotRecovery.restore(store, log, RaftVolatileState(), RaftSnapshotApplier.NoOp)
            }
        }
    }

    @Test
    fun checksumDetectsPayloadBitFlip() = runTest {
        directory { dir ->
            val path = dir.resolve("snapshot")
            val store = FileRaftSnapshotStore(path.file())
            store.save(boundary, byteArrayOf(1, 2, 3))
            val bytes = Files.readAllBytes(path)
            bytes[25] = (bytes[25].toInt() xor 1).toByte()
            Files.write(path, bytes)
            assertFailsWith<RaftSnapshotCorruptionException> { store.load() }
        }
    }

    @Test
    fun legacySnapshotVersionRemainsReadable() = runTest {
        directory { dir ->
            val path = dir.resolve("snapshot")
            val store = FileRaftSnapshotStore(path.file())
            store.save(boundary, byteArrayOf(1, 2, 3))
            val legacy = Files.readAllBytes(path).dropLast(4).toByteArray()
            legacy[4] = 1
            Files.write(path, legacy)
            val (metadata, data) = FileRaftSnapshotStore.open(path.file()).load()!!
            assertEquals(boundary, metadata)
            assertContentEquals(byteArrayOf(1, 2, 3), data)
        }
    }

    @Test
    fun runtimeRejectsUnappliedSnapshotBeforePublishing() = runTest {
        directory { dir ->
            val log = FileRaftLog.open(dir.resolve("log").file())
            for (i in 1L..3L) log.append(entry(i))
            val store = FileRaftSnapshotStore(dir.resolve("snapshot").file())
            val machine = object : RaftStateMachine { override suspend fun apply(command: RaftCommand) {} }
            val node = RaftNodeBuilder("f", setOf("l"), MemoryRaftPersistentStateStore(RaftPersistentState(2)),
                RaftVolatileState(commitIndex = 3, lastApplied = 2), log, machine).build()
            val runtime = RaftRuntime(node, LocalRaftTransport(), snapshotStore = store)
            assertFailsWith<IllegalStateException> { runtime.saveSnapshot(boundary, byteArrayOf(1)) }
            assertNull(store.load())
            assertNotNull(log.get(1))
            runtime.stop()
        }
    }

    private suspend fun directory(block: suspend (Path) -> Unit) {
        val path = Files.createTempDirectory("fortis-snapshot-hardening-")
        try { block(path) } finally {
            Files.walk(path).use { entries -> entries.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
        }
    }
}
