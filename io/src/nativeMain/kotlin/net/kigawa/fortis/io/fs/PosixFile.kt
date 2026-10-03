@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package net.kigawa.fortis.io.fs

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import platform.posix.*

internal suspend fun FortisFile.withPosixFile(
    flags: Int,
    block: suspend (Int) -> Unit,
) {
    withContext(Dispatchers.Default) {
        val fd = open(posixPath(), flags, 420) // 0644, restricted further by umask.
        if (fd < 0) {
            if (errno == ENOENT) throw FsFileNotFoundException(this@withPosixFile)
            throw posixError("open")
        }
        try {
            block(fd)
        } finally {
            close(fd)
        }
    }
}

internal fun FortisFile.posixPath(): String = path.toString().ifEmpty { "." }
internal fun FortisFile.posixError(operation: String): FsException =
    FsException("$operation failed for ${path}: errno=$errno")

internal class PosixInput(override val file: FortisFile, private val fd: Int) : FsInput {
    override suspend fun readAt(offset: Long, buffer: ByteArray, bufferOffset: Int, length: Int): Int {
        require(offset >= 0 && bufferOffset >= 0 && length >= 0 && bufferOffset <= buffer.size - length)
        if (length == 0) return 0
        return withContext(Dispatchers.Default) {
            buffer.usePinned { pinned ->
                var result: Long
                do { result = pread(fd, pinned.addressOf(bufferOffset), length.toULong(), offset) }
                while (result < 0 && errno == EINTR)
                if (result < 0) throw file.posixError("pread")
                result.toInt()
            }
        }
    }

    override suspend fun size(): Long = withContext(Dispatchers.Default) {
        // All data operations are positional, so the descriptor cursor is not otherwise used.
        val result = lseek(fd, 0, SEEK_END)
        if (result < 0) throw file.posixError("lseek")
        result
    }
}

internal class PosixOutput(override val file: FortisFile, private val fd: Int) : FsOutput {
    override suspend fun writeAt(offset: Long, data: ByteArray, dataOffset: Int, length: Int): Int {
        require(offset >= 0 && dataOffset >= 0 && length >= 0 && dataOffset <= data.size - length)
        if (length == 0) return 0
        return withContext(Dispatchers.Default) {
            data.usePinned { pinned ->
                var result: Long
                do { result = pwrite(fd, pinned.addressOf(dataOffset), length.toULong(), offset) }
                while (result < 0 && errno == EINTR)
                if (result < 0) throw file.posixError("pwrite")
                result.toInt()
            }
        }
    }

    override suspend fun writeAppend(data: ByteArray, dataOffset: Int, length: Int): Int =
        writeAt(PosixInput(file, fd).size(), data, dataOffset, length)

    override suspend fun sync() {
        withContext(Dispatchers.Default) { file.syncPosix(fd) }
    }

    override suspend fun truncate(size: Long) {
        require(size >= 0)
        withContext(Dispatchers.Default) {
            if (ftruncate(fd, size) < 0) throw file.posixError("ftruncate")
        }
    }
}

internal fun FortisFile.syncPosix(fd: Int) {
    var result: Int
    do { result = fsync(fd) } while (result < 0 && errno == EINTR)
    if (result < 0) throw posixError("fsync")
}
