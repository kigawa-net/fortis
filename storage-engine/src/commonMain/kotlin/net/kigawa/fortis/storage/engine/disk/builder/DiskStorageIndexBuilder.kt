package net.kigawa.fortis.storage.engine.disk.builder

import net.kigawa.fortis.io.fs.FsInput
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskCorruptionException
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec

data class DiskStorageIndexBuilder(
    val input: FsInput,
    val diskCodec: DiskCodec,
) {
    val indexReader = DiskStorageIndexReader(
        input = input,
    )

    suspend fun buildIndex(index: MutableMap<ByteArrayKey, DiskIndexEntry>): Long {
        val fileSize = input.size()
        var offset = 0L

        while (offset < fileSize) {
            offset += readRecord(fileSize, offset, index)
        }

        return offset
    }

    private suspend fun readRecord(
        fileSize: Long, offset: Long, index: MutableMap<ByteArrayKey, DiskIndexEntry>,
    ): Long {
        validateFileSize(fileSize, offset, diskCodec.headerSize.toLong())

        val headerBytes = ByteArray(diskCodec.headerSize)
        indexReader.readFully(offset, headerBytes)
        val header = readHeader(headerBytes, offset)
        val payloadSize = header.keyLength.toLong() + maxOf(header.valueLength, 0).toLong()
        val recordSize = diskCodec.headerSize.toLong() + payloadSize

        validateFileSize(fileSize, offset, recordSize)

        val key = ByteArray(header.keyLength)
        indexReader.readFully(offset + diskCodec.headerSize, key)
        header.operation.execute(index, key, offset, diskCodec, header)
        return recordSize
    }

    private fun readHeader(headerBytes: ByteArray, offset: Long) = try {
        diskCodec.decoder.decodeHeader(headerBytes)
    } catch (e: IllegalArgumentException) {
        throw DiskCorruptionException(
            "Invalid disk record at offset $offset",
            e,
        )
    }

    fun validateFileSize(fileSize: Long, offset: Long, reqSize: Long) {
        if (fileSize - offset < reqSize) {
            throw DiskCorruptionException(
                "Incomplete disk record at offset $offset"
            )
        }
    }
}