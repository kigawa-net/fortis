package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

data class JvmFsOutput(
    override val file: FortisFile,
    val channel: FileChannel,
): FsOutput {
    override suspend fun writeAt(offset: FsOffset, data: ByteArray): Int = withContext(Dispatchers.IO) {
        channel.write(ByteBuffer.wrap(data), offset)
    }

    override suspend fun writeAppend(data: ByteArray): Int = withContext(Dispatchers.IO) {
        writeAt(channel.size(), data)
    }

    override suspend fun sync() = withContext(Dispatchers.IO) {
        channel.force(true)
    }


    override suspend fun truncate(size: FsByteSize) {
        withContext(Dispatchers.IO) {
            channel.truncate(size)
        }
    }

}