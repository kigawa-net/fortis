package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

data class JvmFsInput(
    override val file: FortisFile,
    val channel: FileChannel,
): FsInput {
    override suspend fun readAt(
        offset: FsOffset,
        buffer: ByteArray,
        bufferOffset: Int,
        length: Int,
    ): Int = withContext(Dispatchers.IO) {
        channel.read(
            ByteBuffer.wrap(buffer, bufferOffset, length),
            offset,
        )
    }

    override suspend fun size(): FsByteSize {
        return withContext(Dispatchers.IO) {
            channel.size().toFsByteSize()
        }
    }


}