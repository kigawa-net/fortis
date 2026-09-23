package net.kigawa.fortis.storage.engine.disk.builder

import net.kigawa.fortis.io.fs.FsInput
import net.kigawa.fortis.storage.engine.disk.DiskCorruptionException

data class DiskStorageIndexReader(
    val input: FsInput,
) {

    suspend fun readFully(offset: Long, buffer: ByteArray) {
        var readTotal = 0

        while (readTotal < buffer.size) {
            val read = input.readAt(
                offset = offset + readTotal,
                buffer = buffer,
                bufferOffset = readTotal,
                length = buffer.size - readTotal,
            )
            if (read <= 0) {
                throw DiskCorruptionException(
                    "Unexpected EOF at offset ${offset + readTotal}"
                )
            }
            readTotal += read
        }
    }
}