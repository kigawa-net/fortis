package net.kigawa.fortis.storage.engine.wal.file

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.storage.engine.wal.WalCodec
import net.kigawa.fortis.storage.engine.wal.WalOperation
import net.kigawa.fortis.storage.engine.wal.WalRecord
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FileWalTest {
    private val codec = WalCodec()

    @Test
    fun appendCanBeReplayedByNewInstance() = runTest {
        withWalPath { path ->
            val record = put(1)

            wal(path).append(record)

            assertEquals(listOf(record), replay(path))
        }
    }

    @Test
    fun multipleRecordsReplayInAppendOrder() = runTest {
        withWalPath { path ->
            val records = listOf(put(1), put(2), WalRecord(3, WalOperation.DELETE, byteArrayOf(1), null))
            val writer = wal(path)

            for (record in records) writer.append(record)

            assertEquals(records, replay(path))
        }
    }

    @Test
    fun partialTailStopsReplayAfterCompleteRecords() = runTest {
        withWalPath { path ->
            val complete = put(1)
            val tail = codec.encode(put(2))
            for (length in listOf(codec.headerSize - 1, tail.size - 1)) {
                withContext(Dispatchers.IO) {
                    Files.write(path, codec.encode(complete) + tail.copyOf(length))
                }

                assertEquals(listOf(complete), replay(path), "Tail length: $length")
            }
        }
    }

    @Test
    fun corruptedRecordFailsWithoutReplayingFollowingRecords() = runTest {
        withWalPath { path ->
            val first = put(1)
            val prefix = codec.encode(first)
            val corrupted = codec.encode(put(2)).also { it[0] = 0 }
            withContext(Dispatchers.IO) {
                Files.write(path, prefix + corrupted + codec.encode(put(3)))
            }
            val replayed = mutableListOf<WalRecord>()

            val error = assertFailsWith<IllegalStateException> {
                wal(path).replay { replayed.add(it) }
            }

            assertEquals(listOf(first), replayed)
            assertEquals("Corrupted WAL at offset ${prefix.size}: Invalid WAL magic", error.message)
        }
    }

    @Test
    fun concurrentAppendsKeepEveryRecordIntact() = runTest {
        withWalPath { path ->
            val writer = wal(path)
            val records = (1L..100L).map { put(it) }

            coroutineScope {
                for (record in records) {
                    launch(Dispatchers.Default) { writer.append(record) }
                }
            }

            val replayed = replay(path)
            assertEquals(records.size, replayed.size)
            assertEquals(records, replayed.sortedBy { it.sequence })
        }
    }

    @Test
    fun syncCreatesEmptyFileAndPreservesAppendedRecords() = runTest {
        withWalPath { path ->
            val writer = wal(path)
            writer.sync()

            withContext(Dispatchers.IO) {
                assertTrue(Files.exists(path))
                assertEquals(0L, Files.size(path))
            }
            assertEquals(emptyList(), replay(path))

            val record = put(1)
            writer.append(record)
            writer.sync()
            writer.sync()

            val bytes = withContext(Dispatchers.IO) { Files.readAllBytes(path) }
            assertContentEquals(codec.encode(record), bytes)
            assertEquals(listOf(record), replay(path))
        }
    }

    private fun put(sequence: Long) = WalRecord(
        sequence, WalOperation.PUT, byteArrayOf(sequence.toByte()), ByteArray(1024) { sequence.toByte() },
    )

    private fun wal(path: Path): FileWal {
        val absolute = path.toAbsolutePath()
        val fsPath = FsPath(absolute.map { FsPath.Element.Name(it.toString()) }, isAbsolute = true)
        return FileWal(fsPath.toFile(), codec)
    }

    private suspend fun replay(path: Path): List<WalRecord> {
        val records = mutableListOf<WalRecord>()
        wal(path).replay { records.add(it) }
        return records
    }

    private suspend fun withWalPath(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-file-wal-test-")
        }
        val path = directory.resolve("records.wal")
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
