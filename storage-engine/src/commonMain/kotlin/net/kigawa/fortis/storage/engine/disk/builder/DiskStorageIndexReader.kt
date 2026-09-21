package net.kigawa.fortis.storage.engine.disk.builder

import net.kigawa.fortis.io.fs.FsInput
import net.kigawa.fortis.storage.engine.disk.DiskCorruptionException

data class DiskStorageIndexReader(
    private val input: FsInput,
    private val offset: Long,
    private val buffer: ByteArray,
) {

    suspend fun readFully() {
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as DiskStorageIndexReader

        if (offset != other.offset) return false
        if (input != other.input) return false
        if (!buffer.contentEquals(other.buffer)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = offset.hashCode()
        result = 31 * result + input.hashCode()
        result = 31 * result + buffer.contentHashCode()
        return result
    }
}