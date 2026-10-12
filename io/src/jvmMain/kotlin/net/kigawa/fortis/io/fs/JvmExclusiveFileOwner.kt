package net.kigawa.fortis.io.fs

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
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
) : AutoCloseable {
    private val closed = AtomicBoolean(false)

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
            val canonicalDirectory = directory.toRealPath(LinkOption.NOFOLLOW_LINKS)
            if (Files.isSymbolicLink(canonicalDirectory)) {
                throw IllegalArgumentException("Symlinked replica data root is not allowed")
            }
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
                return JvmExclusiveFileOwner(canonicalDirectory, channel, lock)
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
