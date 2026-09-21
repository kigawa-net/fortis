package net.kigawa.fortis.storage.engine.disk.builder

import net.kigawa.fortis.io.fs.FsInput
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskCorruptionException
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec
import net.kigawa.fortis.storage.engine.disk.codec.DiskOperation

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
            if (fileSize - offset < diskCodec.headerSize) {
                throw DiskCorruptionException(
                    "Incomplete disk record header at offset $offset"
                )
            }

            val headerBytes = ByteArray(diskCodec.headerSize)

            indexReader.readFully(offset, headerBytes)

            val header =
                try {
                    diskCodec.decoder.decodeHeader(headerBytes)
                } catch (e: IllegalArgumentException) {
                    throw DiskCorruptionException(
                        "Invalid disk record at offset $offset",
                        e,
                    )
                }

            val payloadSize =
                header.keyLength.toLong() +
                    maxOf(header.valueLength, 0).toLong()

            val recordSize =
                diskCodec.headerSize.toLong() +
                    payloadSize

            if (recordSize > fileSize - offset) {
                throw DiskCorruptionException(
                    "Incomplete disk record at offset $offset"
                )
            }

            val key = ByteArray(header.keyLength)

            indexReader.readFully(offset + diskCodec.headerSize, key)

            when (header.operation) {
                DiskOperation.PUT -> {
                    index[ByteArrayKey(key)] =
                        DiskIndexEntry(
                            valueOffset =
                                offset +
                                    diskCodec.headerSize +
                                    header.keyLength,
                            valueLength =
                                header.valueLength,
                        )
                }

                DiskOperation.DELETE -> {
                    index.remove(
                        ByteArrayKey(key)
                    )
                }
            }

            offset += recordSize
        }

        return offset
    }
}