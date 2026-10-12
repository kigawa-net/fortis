package net.kigawa.fortis.io.fs

import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class JvmExclusiveFileOwnerTest {
    @Test
    fun secondOwnerRejectedAndNewOwnerAllowedAfterClose() {
        val directory = Files.createTempDirectory("fortis-writer-owner-")
        try {
            val first = JvmExclusiveFileOwner.tryAcquire(directory)
            try {
                assertFailsWith<WriterAlreadyOwned> {
                    JvmExclusiveFileOwner.tryAcquire(directory)
                }
                assertTrue(Files.exists(directory.resolve(".fortis-writer.lock")))
            } finally {
                first.close()
                first.close() // closing a held owner must be idempotent
            }
            JvmExclusiveFileOwner.tryAcquire(directory).use { reopened ->
                assertTrue(reopened.dataDirectory.toFile().isDirectory)
            }
        } finally {
            Files.deleteIfExists(directory.resolve(".fortis-writer.lock"))
            Files.deleteIfExists(directory)
        }
    }

    @Test
    fun symlinkLockIsRejectedBeforeFileOpen() {
        val directory = Files.createTempDirectory("fortis-writer-symlink-")
        val other = Files.createTempFile("fortis-other-lock-", ".tmp")
        try {
            try {
                Files.createSymbolicLink(directory.resolve(".fortis-writer.lock"), other)
            } catch (_: UnsupportedOperationException) {
                return // filesystem without symlink support
            }
            assertFailsWith<IllegalArgumentException> {
                JvmExclusiveFileOwner.tryAcquire(directory)
            }
        } finally {
            Files.deleteIfExists(directory.resolve(".fortis-writer.lock"))
            Files.deleteIfExists(directory)
            Files.deleteIfExists(other)
        }
    }

    @Test
    fun missingDirectoryIsRejectedWithoutCreatingDataRoot() {
        val parent = Files.createTempDirectory("fortis-writer-missing-")
        try {
            val missing: Path = parent.resolve("not-created")
            assertFailsWith<IllegalArgumentException> {
                JvmExclusiveFileOwner.tryAcquire(missing)
            }
            assertTrue(Files.notExists(missing))
        } finally {
            Files.deleteIfExists(parent)
        }
    }
}
