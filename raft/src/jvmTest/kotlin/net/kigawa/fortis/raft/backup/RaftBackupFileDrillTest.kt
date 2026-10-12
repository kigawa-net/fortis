package net.kigawa.fortis.raft.backup

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.FileRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.log.FileRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RaftBackupFileDrillTest {
    private val codec = RaftBackupCodec()

    @Test
    fun fileBackupRestoreDrill() = runTest {
        withDirectory { directory ->
            val sourceLog = FileRaftLog.open(directory.resolve("raft.log").toFortisFile())
            val sourceStore = FileRaftPersistentStateStore(
                directory.resolve("raft.state").toFortisFile(),
            )
            sourceStore.save(3, "node-2")
            sourceLog.append(
                RaftLogEntry(
                    index = 1,
                    term = 2,
                    command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10)),
                ),
            )
            sourceLog.append(
                RaftLogEntry(
                    index = 2,
                    term = 3,
                    command = RaftCommand.Delete(byteArrayOf(1)),
                ),
            )

            val exported = codec.export(
                RaftBackupProcedures.backup(
                    sourceLog,
                    sourceStore,
                    upToIndex = 2,
                    metadata = SnapshotMetadata(1, 2),
                ),
            )
            val backup = codec.import(exported)

            val targetLog = FileRaftLog.open(directory.resolve("restored.log").toFortisFile())
            val targetStore = FileRaftPersistentStateStore(
                directory.resolve("restored.state").toFortisFile(),
            )
            RaftBackupProcedures.restore(backup, targetLog, targetStore)

            assertEquals(3L, targetStore.load().currentTerm)
            assertEquals("node-2", targetStore.load().votedFor)
            assertEquals(2L, targetLog.lastIndex())
            assertEquals(sourceLog.get(1), targetLog.get(1))
            assertEquals(sourceLog.get(2), targetLog.get(2))

            targetLog.append(
                RaftLogEntry(
                    index = 3,
                    term = 3,
                    command = RaftCommand.Put(byteArrayOf(2), byteArrayOf(20)),
                ),
            )
            assertEquals(3L, targetLog.lastIndex())
        }
    }

    @Test
    fun corruptedFileBackupRestoreFails() = runTest {
        withDirectory { directory ->
            val sourceLog = FileRaftLog.open(directory.resolve("raft.log").toFortisFile())
            val sourceStore = FileRaftPersistentStateStore(
                directory.resolve("raft.state").toFortisFile(),
            )
            sourceStore.save(1, null)
            sourceLog.append(
                RaftLogEntry(
                    index = 1,
                    term = 1,
                    command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(1)),
                ),
            )
            val exported = codec.export(
                RaftBackupProcedures.backup(sourceLog, sourceStore, upToIndex = 1),
            )
            val truncated = exported.copyOf(exported.size - 1)
            assertFailsWith<RaftBackupCorruptionException> { codec.import(truncated) }
        }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withDirectory(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-raft-backup-test-")
        }
        try {
            block(directory)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.list(directory).use { entries ->
                    entries.forEach { Files.deleteIfExists(it) }
                }
                Files.deleteIfExists(directory)
            }
        }
    }
}
