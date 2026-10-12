package net.kigawa.fortis.io.fs

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean

/**
 * JVM-local filesystem writer ownership prototype for fortis#53.
 *
 * This is NOT wired to WAL/Raft constructors and is NOT a production safety claim.
 * In particular, Native interoperability, network filesystems, aliasing and Kubernetes
 * overlap must be verified before this can guard writable production storage.
 */
internal class JvmExclusiveFileOwner private constructor(
    val dataDirectory: Path,
    private val channel: FileChannel,
    private val lock: FileLock,
    private val lockFileKey: Any,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)

    /**
     * Advisory diagnostic/fail-closed check, not an atomic fencing mechanism:
     * a malicious replacement can race with the following disk operation.
     */
    fun verifyLockFileIdentity() {
        if (closed.get() || !lock.isValid) {
            throw IOException("Replica writer lock is no longer held: $dataDirectory")
        }
        val current = try {
            Files.readAttributes(
                dataDirectory.resolve(".fortis-writer.lock"),
                BasicFileAttributes::class.java,
                LinkOption.NOFOLLOW_LINKS,
            )
        } catch (failure: IOException) {
            throw IOException("Replica writer lock path is unavailable: $dataDirectory", failure)
        }
        if (!current.isRegularFile || current.fileKey() != lockFileKey) {
            throw IOException("Replica writer lock file identity has changed: $dataDirectory")
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                lock.release()
            } finally {
                channel.close()
            }
        }
    }

    companion object {
        private const val LOCK_FILE = ".fortis-writer.lock"

        /**
         * Never opens WAL, RaftLog or Snapshot before acquiring ownership.
         * Only accepts an already-existing real directory; does not resolve/permit
         * a symlinked data root or pre-existing symlinked lock file.
         */
        fun tryAcquire(directory: Path): JvmExclusiveFileOwner {
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw IllegalArgumentException("Replica data root must be an existing non-symlink directory")
            }
            // The leaf directory is already rejected if symlinked. Resolve symlinked
            // ANCESTORS to a stable physical path so distinct path aliases cannot
            // masquerade as different replica directories.
            val canonicalDirectory = directory.toRealPath()
            val lockPath = canonicalDirectory.resolve(LOCK_FILE)
            if (Files.isSymbolicLink(lockPath)) {
                throw IllegalArgumentException("Symlinked writer lock file is not allowed")
            }
            val channel = FileChannel.open(
                lockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS,
            )
            try {
                val lock = try {
                    channel.tryLock(0L, 1L, false)
                } catch (_: OverlappingFileLockException) {
                    null
                }
                if (lock == null) throw WriterAlreadyOwned(canonicalDirectory)
                val attributes = Files.readAttributes(
                    lockPath,
                    BasicFileAttributes::class.java,
                    LinkOption.NOFOLLOW_LINKS,
                )
                val fileKey = attributes.fileKey()
                    ?: throw IOException("Filesystem cannot verify writer lock file identity: $lockPath")
                if (!attributes.isRegularFile) {
                    throw IOException("Writer lock path is not a regular file: $lockPath")
                }
                return JvmExclusiveFileOwner(canonicalDirectory, channel, lock, fileKey)
                    .also { it.verifyLockFileIdentity() }
            } catch (failure: Throwable) {
                try {
                    channel.close()
                } catch (closeFailure: Throwable) {
                    failure.addSuppressed(closeFailure)
                }
                throw failure
            }
        }
    }
}

internal class WriterAlreadyOwned(directory: Path) :
    IOException("Another durable writer owns replica directory: $directory")
