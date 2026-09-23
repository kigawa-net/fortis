package net.kigawa.fortis.raft.log

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class FileRaftLogTest {
    private val codec = RaftLogCodec()

    @Test
    fun newFileStartsWithEmptyLog() = runTest {
        withLogPath { path ->
            val log = open(path)

            assertEquals(0L, log.lastIndex())
            assertNull(log.get(1))
        }
    }

    @Test
    fun appendThenGetReturnsEntries() = runTest {
        withLogPath { path ->
            val log = open(path)
            val entries = listOf(put(1, 1), delete(2, 2), put(3, 2))

            for (entry in entries) log.append(entry)

            assertEquals(3L, log.lastIndex())
            for (entry in entries) assertEquals(entry, log.get(entry.index))
            assertNull(log.get(0))
            assertNull(log.get(4))
        }
    }

    @Test
    fun restartRebuildsIndexAndRestoresEntries() = runTest {
        withLogPath { path ->
            val entries = listOf(put(1, 1), delete(2, 2), put(3, 2))
            val writer = open(path)
            for (entry in entries) writer.append(entry)

            val rebuilt = open(path)

            assertEquals(3L, rebuilt.lastIndex())
            for (entry in entries) assertEquals(entry, rebuilt.get(entry.index))
        }
    }

    @Test
    fun truncateMiddleRemovesEntriesFromFile() = runTest {
        withLogPath { path ->
            val log = logWithEntries(path, 4)

            log.truncateFrom(3)

            assertEquals(2L, log.lastIndex())
            assertNull(log.get(3))
            assertEquals(2L, open(path).lastIndex())
        }
    }

    @Test
    fun truncateFirstEntryEmptiesLog() = runTest {
        withLogPath { path ->
            val log = logWithEntries(path, 3)

            log.truncateFrom(1)

            assertEquals(0L, log.lastIndex())
            assertEquals(0L, open(path).lastIndex())
        }
    }

    @Test
    fun truncateFutureIndexDoesNothing() = runTest {
        withLogPath { path ->
            val log = logWithEntries(path, 2)

            log.truncateFrom(10)

            assertEquals(2L, log.lastIndex())
            assertEquals(2L, open(path).lastIndex())
        }
    }

    @Test
    fun truncateThenAppendPersistsReplacement() = runTest {
        withLogPath { path ->
            val log = logWithEntries(path, 3)
            val replacement = delete(2, 5)

            log.truncateFrom(2)
            log.append(replacement)

            val rebuilt = open(path)
            assertEquals(2L, rebuilt.lastIndex())
            assertEquals(replacement, rebuilt.get(2))
            assertNull(rebuilt.get(3))
        }
    }

    @Test
    fun incompleteHeaderTailIsDiscardedOnRestart() = runTest {
        withLogPath { path ->
            val complete = codec.encode(put(1, 1))
            val incomplete = codec.encode(put(2, 1)).copyOf(codec.headerSize - 1)
            write(path, complete + incomplete)

            val rebuilt = open(path)

            assertEquals(1L, rebuilt.lastIndex())
            assertEquals(complete.size.toLong(), Files.size(path))
        }
    }

    @Test
    fun incompletePayloadTailIsDiscardedAndCanBeReplaced() = runTest {
        withLogPath { path ->
            val first = put(1, 1)
            val complete = codec.encode(first)
            val second = codec.encode(put(2, 1))
            write(path, complete + second.copyOf(second.size - 1))

            val rebuilt = open(path)
            val replacement = delete(2, 2)
            rebuilt.append(replacement)

            assertEquals(replacement, open(path).get(2))
        }
    }

    @Test
    fun corruptedRecordFailsDuringRestart() = runTest {
        withLogPath { path ->
            val corrupted = codec.encode(put(1, 1)).also { it[0] = 0 }
            write(path, corrupted)

            assertFailsWith<RaftLogCorruptionException> { open(path) }
        }
    }

    @Test
    fun nonContiguousPersistedIndexFailsDuringRestart() = runTest {
        withLogPath { path ->
            write(path, codec.encode(put(1, 1)) + codec.encode(put(3, 1)))

            assertFailsWith<RaftLogCorruptionException> { open(path) }
        }
    }

    private suspend fun logWithEntries(path: Path, count: Int): FileRaftLog {
        val log = open(path)
        for (index in 1L..count.toLong()) {
            log.append(put(index, index))
        }
        return log
    }

    private fun put(index: Long, term: Long) = RaftLogEntry(
        index,
        term,
        RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(term.toByte())),
    )

    private fun delete(index: Long, term: Long) = RaftLogEntry(
        index,
        term,
        RaftCommand.Delete(byteArrayOf(index.toByte())),
    )

    private suspend fun open(path: Path): FileRaftLog =
        FileRaftLog.open(path.toFortisFile(), codec)

    private suspend fun write(path: Path, bytes: ByteArray) {
        withContext(Dispatchers.IO) { Files.write(path, bytes) }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withLogPath(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-raft-log-test-")
        }
        val path = directory.resolve("raft.log")
        try {
            block(path)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.deleteIfExists(path)
                Files.deleteIfExists(directory)
            }
        }
    }
}
