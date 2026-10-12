package net.kigawa.fortis.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsException
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import net.kigawa.fortis.raft.vote.RequestVoteHandler
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class FileRaftPersistentStateCrashTest {
    private val file = FsPath(listOf(FsPath.Element.Name("state")), false).toFile()
    private val temporary = FsPath(listOf(FsPath.Element.Name("state.tmp")), false).toFile()
    private val codec = RaftPersistentStateCodec()
    private fun store(fs: Filesystem) = FileRaftPersistentStateStore(file, codec, fs)

    @Test
    fun publishesOnlyAfterFullWriteFileSyncRenameAndDirectorySync() = runTest {
        val fs = Filesystem()
        store(fs).save(3, "node-2")
        assertEquals(listOf("write:state.tmp", "fileSync:state.tmp", "rename", "directorySync"), fs.events)
        assertFalse(temporary in fs.visible)
        fs.powerLoss()
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
    }

    @Test
    fun partialWriteCrashKeepsAcknowledgedState() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        fs.failure = "write"
        assertFailsWith<FsException> { store.save(4, "node-3") }
        assertEquals(RaftPersistentState(3, "node-2"), decode(fs.visible.getValue(file)))
        fs.powerLoss()
        fs.failure = null
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
    }

    @Test
    fun temporaryFileSyncFailureDoesNotTruncateCanonicalState() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        val old = fs.visible.getValue(file).copyOf()
        fs.failure = "fileSync"
        assertFailsWith<FsException> { store.save(4, null) }
        assertContentEquals(old, fs.visible[file])
        fs.failure = null
        assertEquals(RaftPersistentState(3, "node-2"), store.load())
        store.save(5, null)
        fs.powerLoss()
        assertEquals(RaftPersistentState(5, null), store(fs).load())
    }

    @Test
    fun renameFailureCannotBeAcknowledgedOrUsedToContinueVoting() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        fs.failure = "rename"
        assertFailsWith<FsException> { store.save(4, null) }
        assertFailsWith<RaftPersistentStatePersistenceException> { store.load() }
        assertFailsWith<RaftPersistentStatePersistenceException> { store.save(4, "node-3") }
        fs.powerLoss()
        fs.failure = null
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
    }

    @Test
    fun directorySyncFailureCanRecoverOldCompleteStateAfterPowerLoss() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        fs.failure = "directorySync"
        assertFailsWith<FsException> { store.save(4, "node-3") }
        assertEquals(RaftPersistentState(4, "node-3"), decode(fs.visible.getValue(file)))
        assertFailsWith<RaftPersistentStatePersistenceException> { store.load() }
        fs.powerLoss()
        fs.failure = null
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
    }

    @Test
    fun processRestartAfterDirectorySyncFailureMakesVisibleNewStateDurable() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        fs.failure = "directorySync"
        assertFailsWith<FsException> { store.save(4, "node-3") }
        assertFailsWith<RaftPersistentStatePersistenceException> { store.save(5, null) }
        fs.failure = null
        assertEquals(RaftPersistentState(4, "node-3"), store(fs).load())
        fs.powerLoss()
        assertEquals(RaftPersistentState(4, "node-3"), store(fs).load())
    }

    @Test
    fun voteResponseWaitsUntilDirectorySyncCompletes() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        val state = store.load()
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        fs.beforeDirectorySync = { entered.complete(Unit); release.await() }
        val vote = async {
            RequestVoteHandler(state, store, MemoryRaftLog()).handle(RequestVoteRequest(4, "node-3", 0, 0))
        }
        runCurrent()
        assertTrue(entered.isCompleted)
        assertTrue(vote.isActive)
        assertEquals(RaftPersistentState(3, "node-2"), state)
        release.complete(Unit)
        assertTrue(vote.await().voteGranted)
        assertEquals(RaftPersistentState(4, "node-3"), state)
        fs.beforeDirectorySync = {}
        fs.powerLoss()
        val restored = store(fs).load()
        val rejected = RequestVoteHandler(restored, store(fs), MemoryRaftLog())
            .handle(RequestVoteRequest(4, "node-4", 0, 0))
        assertFalse(rejected.voteGranted)
        assertEquals(RaftPersistentState(4, "node-3"), restored)
    }

    @Test
    fun cancellationAfterRenameStillCompletesDirectorySync() = runTest {
        val fs = Filesystem()
        val store = store(fs)
        store.save(3, "node-2")
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        fs.beforeDirectorySync = { entered.complete(Unit); release.await() }
        val save = launch { store.save(4, "node-3") }
        runCurrent()
        assertTrue(entered.isCompleted)
        save.cancel()
        runCurrent()
        assertFalse(save.isCompleted)
        release.complete(Unit)
        save.join()
        fs.beforeDirectorySync = {}
        fs.powerLoss()
        assertEquals(RaftPersistentState(4, "node-3"), store(fs).load())
    }

    @Test
    fun completeTemporaryOnlyStateIsSyncedAndPromotedOnRestart() = runTest {
        val fs = Filesystem()
        fs.visible[temporary] = codec.encode(RaftPersistentState(7, "node-2"))
        assertEquals(RaftPersistentState(7, "node-2"), store(fs).load())
        assertEquals(listOf("fileSync:state.tmp", "rename", "directorySync"), fs.events)
        assertFalse(temporary in fs.visible)
        fs.powerLoss()
        assertEquals(RaftPersistentState(7, "node-2"), store(fs).load())
    }

    @Test
    fun incompleteTemporaryOnlyStateNeverDefaultsToTermZero() = runTest {
        val fs = Filesystem()
        fs.visible[temporary] = codec.encode(RaftPersistentState(7, "node-2")).copyOf(5)
        assertFailsWith<RaftPersistentStateCorruptionException> { store(fs).load() }
        assertFailsWith<RaftPersistentStateCorruptionException> { store(fs).save(1, null) }
        assertFalse(file in fs.visible)
        assertEquals(emptyList(), fs.events)
    }

    @Test
    fun canonicalStateWinsOverUncommittedHigherTermTemporaryState() = runTest {
        val fs = Filesystem()
        store(fs).save(3, "node-2")
        fs.visible[temporary] = codec.encode(RaftPersistentState(4, "node-3"))
        val restored = store(fs)
        assertEquals(RaftPersistentState(3, "node-2"), restored.load())
        assertFalse(temporary in fs.visible)
        restored.save(4, "node-4")
        fs.powerLoss()
        assertEquals(RaftPersistentState(4, "node-4"), store(fs).load())
    }

    @Test
    fun canonicalCorruptionIsNotHiddenByValidTemporaryState() = runTest {
        val fs = Filesystem()
        val corrupt = codec.encode(RaftPersistentState(3, "node-2")).also { it[0] = 0 }
        fs.visible[file] = corrupt
        fs.visible[temporary] = codec.encode(RaftPersistentState(4, "node-3"))
        assertFailsWith<RaftPersistentStateCorruptionException> { store(fs).load() }
        assertFailsWith<RaftPersistentStateCorruptionException> { store(fs).save(5, null) }
        assertContentEquals(corrupt, fs.visible[file])
        assertEquals(emptyList(), fs.events)
    }

    @Test
    fun freshStoreAndPreviouslyOpenedStoreCannotDecreasePersistedTerm() = runTest {
        val fs = Filesystem()
        val old = store(fs)
        old.save(3, null)
        store(fs).save(5, "node-2")
        assertFailsWith<IllegalArgumentException> { old.save(4, null) }
        assertFailsWith<IllegalArgumentException> { store(fs).save(2, "node-3") }
        fs.powerLoss()
        assertEquals(RaftPersistentState(5, "node-2"), store(fs).load())
    }

    @Test
    fun sameTermVoteCannotBeClearedOrChangedAcrossRestarts() = runTest {
        val fs = Filesystem()
        store(fs).save(3, null)
        store(fs).save(3, "node-2")
        assertFailsWith<IllegalArgumentException> { store(fs).save(3, null) }
        assertFailsWith<IllegalArgumentException> { store(fs).save(3, "node-3") }
        store(fs).save(3, "node-2")
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
        store(fs).save(4, null)
        assertEquals(RaftPersistentState(4, null), store(fs).load())
    }

    @Test
    fun recoveryDirectorySyncFailureDoesNotReturnStateOrDefault() = runTest {
        val fs = Filesystem()
        store(fs).save(3, "node-2")
        fs.failure = "directorySync"
        assertFailsWith<FsException> { store(fs).load() }
        fs.failure = null
        assertEquals(RaftPersistentState(3, "node-2"), store(fs).load())
    }

    private fun decode(bytes: ByteArray): RaftPersistentState =
        assertIs<net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateDecodeResult.Success>(codec.decode(bytes)).state

    /** Directory entries become durable only at directorySync; powerLoss drops unpublished changes. */
    private class Filesystem : RaftStateFileAccess {
        val visible = mutableMapOf<FortisFile, ByteArray>()
        private var durable = mapOf<FortisFile, ByteArray>()
        val events = mutableListOf<String>()
        var failure: String? = null
        var beforeDirectorySync: suspend () -> Unit = {}
        override suspend fun read(file: FortisFile): ByteArray? = visible[file]?.copyOf()
        override suspend fun writeAndSync(file: FortisFile, data: ByteArray) {
            events.add("write:${file.path}")
            visible[file] = if (failure == "write") data.copyOf(5) else data.copyOf()
            fail("write")
            syncFile(file)
        }
        override suspend fun syncFile(file: FortisFile) {
            events.add("fileSync:${file.path}")
            fail("fileSync")
        }
        override suspend fun atomicReplace(source: FortisFile, target: FortisFile) {
            events.add("rename")
            fail("rename")
            visible[target] = visible.getValue(source)
            visible.remove(source)
        }
        override suspend fun syncDirectory(directory: FortisFile) {
            events.add("directorySync")
            beforeDirectorySync()
            fail("directorySync")
            durable = visible.mapValues { it.value.copyOf() }
        }
        override suspend fun deleteIfExists(file: FortisFile) { visible.remove(file) }
        fun powerLoss() { visible.clear(); visible.putAll(durable.mapValues { it.value.copyOf() }) }
        private fun fail(stage: String) { if (failure == stage) throw FsException("Injected $stage failure") }
    }
}
