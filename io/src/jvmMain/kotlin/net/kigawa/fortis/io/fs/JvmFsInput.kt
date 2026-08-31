package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

data class JvmFsInput(
    override val file: FortisFile,
    val channel: FileChannel,
): FsInput {
    override suspend fun readAt(offset: FsOffset, buffer: ByteArray): Int {
        return withContext(Dispatchers.IO) {
            channel.read(ByteBuffer.wrap(buffer), offset)
        }
    }
}