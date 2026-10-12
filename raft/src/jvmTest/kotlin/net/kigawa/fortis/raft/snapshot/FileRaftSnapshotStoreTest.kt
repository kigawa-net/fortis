package net.kigawa.fortis.raft.snapshot

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [FileRaftSnapshotStore] の保存・読み戻しと Handler 適用との往復を確認する。
 */
class FileRaftSnapshotStoreTest {
    @Test
    fun saveLoadRoundTrip() = runTest {
        withDirectory { directory ->
            val store = FileRaftSnapshotStore(directory.resolve("raft.snapshot").toFortisFile())
            assertNull(store.load())
            assertNull(store.latestMetadata())

            val metadata = SnapshotMetadata(3, 6)
            val data = byteArrayOf(9, 8, 7)
            store.save(metadata, data)

            val loaded = store.load()
            assertEquals(metadata, loaded?.first)
            assertTrue(loaded?.second.contentEquals(data) == true)
            assertEquals(metadata, store.latestMetadata())
        }
    }

    @Test
    fun resaveUpdatesGeneration() = runTest {
        withDirectory { directory ->
            val store = FileRaftSnapshotStore(directory.resolve("raft.snapshot").toFortisFile())
            store.save(SnapshotMetadata(1, 1), byteArrayOf(1))
            store.save(SnapshotMetadata(3, 6), byteArrayOf(2, 3))

            val loaded = store.load()
            assertEquals(SnapshotMetadata(3, 6), loaded?.first)
            assertTrue(loaded?.second.contentEquals(byteArrayOf(2, 3)) == true)
            assertEquals(3L, store.latestMetadata()?.lastIncludedIndex)
        }
    }

    @Test
    fun corruptedFileFailsLoad() = runTest {
        withDirectory { directory ->
            val path = directory.resolve("raft.snapshot")
            path.toFortisFile().openWrite(isCreate = true) { output ->
                output.truncate(0)
                val garbage = byteArrayOf(1, 2, 3, 4, 5)
                var offset = 0
                while (offset < garbage.size) {
                    val written = output.writeAt(offset.toLong(), garbage, offset, garbage.size - offset)
                    check(written > 0)
                    offset += written
                }
                output.sync()
            }

            val store = FileRaftSnapshotStore(path.toFortisFile())
            assertFailsWith<RaftSnapshotCorruptionException> { store.load() }
        }
    }

    @Test
    fun truncatedFileFailsLoad() = runTest {
        withDirectory { directory ->
            val file = directory.resolve("raft.snapshot").toFortisFile()
            val store = FileRaftSnapshotStore(file)
            store.save(SnapshotMetadata(2, 4), byteArrayOf(1, 2, 3))
            file.openWrite(isCreate = true) { output -> output.truncate(10) }

            assertFailsWith<RaftSnapshotCorruptionException> { store.load() }
            assertFailsWith<RaftSnapshotCorruptionException> { FileRaftSnapshotStore.open(file) }
        }
    }

    @Test
    fun handlerPersistsAcrossReopen() = runTest {
        withDirectory { directory ->
            val file = directory.resolve("raft.snapshot").toFortisFile()
            val persistentState = RaftPersistentState(2, "node-9")
            val log = MemoryRaftLog()
            for (index in 1L..3L) {
                log.append(
                    RaftLogEntry(
                        index = index,
                        term = 2,
                        command = RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(1)),
                    ),
                )
            }
            InstallSnapshotHandler(
                persistentState = persistentState,
                persistentStateStore = MemoryRaftPersistentStateStore(persistentState),
                volatileState = RaftVolatileState(),
                log = log,
                timer = RaftTimer.None,
                snapshotApplier = RaftSnapshotApplier.NoOp,
                snapshotStore = FileRaftSnapshotStore(file),
            ).handle(
                InstallSnapshotRequest(
                    term = 3,
                    leaderId = "leader-1",
                    metadata = SnapshotMetadata(3, 3),
                    data = byteArrayOf(9),
                ),
            )

            val reopened = FileRaftSnapshotStore.open(file).load()
            assertEquals(SnapshotMetadata(3, 3), reopened?.first)
            assertTrue(reopened?.second.contentEquals(byteArrayOf(9)) == true)
        }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withDirectory(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-raft-snapshot-test-")
        }
        try {
            block(directory)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.deleteIfExists(directory.resolve("raft.snapshot"))
                Files.deleteIfExists(directory)
            }
        }
    }
}
