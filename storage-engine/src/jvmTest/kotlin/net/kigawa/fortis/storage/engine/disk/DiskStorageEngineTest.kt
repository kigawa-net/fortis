package net.kigawa.fortis.storage.engine.disk

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.storage.engine.disk.builder.DiskStorageEngineBuilder
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DiskStorageEngineTest {
    private val codec = DiskCodec()

    @Test
    fun newFileStartsWithEmptyDatabase() = runTest {
        withDatabasePath { path ->
            assertFalse(Files.exists(path))

            val storage = build(path)

            assertNull(storage.get(byteArrayOf(1)))
        }
    }

    @Test
    fun putThenGetReturnsValue() = runTest {
        withDatabasePath { path ->
            val storage = build(path)

            storage.put(byteArrayOf(1), byteArrayOf(10))

            assertContentEquals(byteArrayOf(10), storage.get(byteArrayOf(1)))
        }
    }

    @Test
    fun putThenRebuildRestoresValue() = runTest {
        withDatabasePath { path ->
            build(path).put(byteArrayOf(1), byteArrayOf(10))

            val rebuilt = build(path)

            assertContentEquals(byteArrayOf(10), rebuilt.get(byteArrayOf(1)))
        }
    }

    @Test
    fun deleteThenRebuildDoesNotRestoreValue() = runTest {
        withDatabasePath { path ->
            val storage = build(path)
            storage.put(byteArrayOf(1), byteArrayOf(10))

            assertTrue(storage.delete(byteArrayOf(1)))

            assertNull(build(path).get(byteArrayOf(1)))
        }
    }

    @Test
    fun invalidMagicFailsAsDiskCorruption() = runTest {
        val record = codec.encoder.encodePut(byteArrayOf(1), byteArrayOf(10))
        record[0] = 0

        assertCorrupted(record)
    }

    @Test
    fun invalidVersionFailsAsDiskCorruption() = runTest {
        val record = codec.encoder.encodePut(byteArrayOf(1), byteArrayOf(10))
        record[4] = (codec.version + 1).toByte()

        assertCorrupted(record)
    }

    @Test
    fun invalidOperationFailsAsDiskCorruption() = runTest {
        val record = codec.encoder.encodePut(byteArrayOf(1), byteArrayOf(10))
        record[5] = Byte.MAX_VALUE

        assertCorrupted(record)
    }

    @Test
    fun truncatedHeaderFailsAsDiskCorruption() = runTest {
        val record = codec.encoder.encodePut(byteArrayOf(1), byteArrayOf(10))

        assertCorrupted(record.copyOf(codec.headerSize - 1))
    }

    @Test
    fun truncatedPayloadFailsAsDiskCorruption() = runTest {
        val record = codec.encoder.encodePut(byteArrayOf(1), byteArrayOf(10))

        assertCorrupted(record.copyOf(record.size - 1))
    }

    @Test
    fun truncatedValueAfterBuildFailsAsDiskCorruption() = runTest {
        withDatabasePath { path ->
            val storage = build(path)
            storage.put(byteArrayOf(1), byteArrayOf(10))
            withContext(Dispatchers.IO) {
                Files.newByteChannel(path, StandardOpenOption.WRITE).use { channel ->
                    channel.truncate(Files.size(path) - 1)
                }
            }

            assertFailsWith<DiskCorruptionException> {
                storage.get(byteArrayOf(1))
            }
        }
    }

    private suspend fun assertCorrupted(bytes: ByteArray) {
        withDatabasePath { path ->
            withContext(Dispatchers.IO) {
                Files.write(path, bytes)
            }

            assertFailsWith<DiskCorruptionException> {
                build(path)
            }
        }
    }

    private suspend fun build(path: Path): DiskStorageEngine =
        DiskStorageEngineBuilder(path.toFortisFile()).build()

    private fun Path.toFortisFile() =
        FsPath(
            elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
            isAbsolute = true,
        ).toFile()

    private suspend fun withDatabasePath(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-disk-storage-test-")
        }
        val path = directory.resolve("storage.db")
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
