package net.kigawa.fortis.io.fs

import java.io.IOException
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertFailsWith

class JvmExclusiveFileOwnerIdentityTest {
    @Test
    fun replacingLockFileWhileHeldIsDetected() {
        val directory = Files.createTempDirectory("fortis-lock-identity-")
        val lockFile = directory.resolve(".fortis-writer.lock")
        val oldFile = directory.resolve("old-lock")
        try {
            JvmExclusiveFileOwner.tryAcquire(directory).use { owner ->
                owner.verifyLockFileIdentity()
                Files.move(lockFile, oldFile)
                try {
                    Files.createFile(lockFile)
                    assertFailsWith<IOException> { owner.verifyLockFileIdentity() }
                } finally {
                    Files.deleteIfExists(lockFile)
                }
            }
        } finally {
            Files.deleteIfExists(oldFile)
            Files.deleteIfExists(lockFile)
            Files.deleteIfExists(directory)
        }
    }

    @Test
    fun removedLockFileWhileHeldIsDetected() {
        val directory = Files.createTempDirectory("fortis-lock-removed-")
        val lockFile = directory.resolve(".fortis-writer.lock")
        try {
            JvmExclusiveFileOwner.tryAcquire(directory).use { owner ->
                Files.delete(lockFile)
                assertFailsWith<IOException> { owner.verifyLockFileIdentity() }
            }
        } finally {
            Files.deleteIfExists(lockFile)
            Files.deleteIfExists(directory)
        }
    }
}
