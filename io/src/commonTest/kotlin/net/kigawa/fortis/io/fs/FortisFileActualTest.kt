package net.kigawa.fortis.io.fs

import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Exercises real filesystem operations on JVM, Linux x64 and macOS ARM64. */
class FortisFileActualTest {
    private fun file(name: String): FortisFile =
        FsPath(listOf(FsPath.Element.Name(name)), isAbsolute = false).toFile()

    private suspend fun FsOutput.writeFully(payload: ByteArray) {
        var written = 0
        while (written < payload.size) {
            val n = writeAt(written.toLong(), payload, written, payload.size - written)
            check(n > 0) { "writeAt made no forward progress" }
            written += n
        }
    }

    @Test
    fun writeReadTruncateRenameAndDirectorySync() = runBlocking {
        val suffix = Random.nextLong().toULong().toString(16)
        val staged = file("fortis-io-stage-$suffix")
        val target = file("fortis-io-target-$suffix")
        val directory = FsPath(emptyList(), isAbsolute = false).toFile()
        val payload = "fortis-native-io-roundtrip".encodeToByteArray()
        try {
            staged.openWrite(isCreate = true) { output ->
                output.writeFully(payload)
                output.sync()
            }
            staged.openRead { input ->
                assertEquals(payload.size.toLong(), input.size())
                val actual = ByteArray(payload.size)
                assertEquals(payload.size, input.readAt(0L, actual, 0, actual.size))
                assertContentEquals(payload, actual)
            }

            staged.atomicReplace(target)
            directory.syncDirectory()

            target.openReadWrite(isCreate = false) { io ->
                assertEquals(payload.size.toLong(), io.input.size())
                io.output.truncate(6L)
                io.output.sync()
            }
            target.openRead { input ->
                assertEquals(6L, input.size())
                val actual = ByteArray(6)
                assertEquals(6, input.readAt(0L, actual, 0, actual.size))
                assertContentEquals(payload.copyOf(6), actual)
            }
        } finally {
            staged.deleteIfExists()
            target.deleteIfExists()
        }
    }

    @Test
    fun nonexistentFileRaisesTypedNotFound() = runBlocking {
        val name = "fortis-io-missing-${Random.nextLong().toULong().toString(16)}"
        val missing = file(name)
        missing.deleteIfExists()
        var notFound = false
        try {
            missing.openRead { _ -> error("unexpected open success") }
        } catch (_: FsFileNotFoundException) {
            notFound = true
        }
        assertTrue(notFound, "a nonexistent path must not return an empty input")
    }
}
