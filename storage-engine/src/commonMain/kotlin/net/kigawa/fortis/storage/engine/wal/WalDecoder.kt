package net.kigawa.fortis.storage.engine.wal

import net.kigawa.fortis.storage.engine.wal.WalCodec.HEADER_SIZE
import net.kigawa.fortis.storage.engine.wal.WalCodec.VERSION

class WalDecoder(
    val data: ByteArray,
    val offset: Int = 0,
) {
    fun decode(
    ): WalDecodeResult {
        if (offset < 0 || offset > data.size) {
            return WalDecodeResult.Corrupted("Invalid offset: $offset")
        }

        if (data.size - offset < HEADER_SIZE) {
            return WalDecodeResult.Incomplete
        }

        if (
            data[offset] != 'F'.code.toByte() ||
            data[offset + 1] != 'W'.code.toByte() ||
            data[offset + 2] != 'A'.code.toByte() ||
            data[offset + 3] != 'L'.code.toByte()
        ) {
            return WalDecodeResult.Corrupted("Invalid WAL magic")
        }

        val version = data[offset + 4]
        if (version != VERSION) {
            return WalDecodeResult.Corrupted(
                "Unsupported WAL version: $version"
            )
        }

        val operation = WalOperation.fromCode(data[offset + 5])
            ?: return WalDecodeResult.Corrupted(
                "Unknown WAL operation: ${data[offset + 5]}"
            )

        val sequence = readLong(data, offset + 6)
        val keyLength = readInt(data, offset + 14)
        val valueLength = readInt(data, offset + 18)

        if (keyLength < 0) {
            return WalDecodeResult.Corrupted(
                "Invalid key length: $keyLength"
            )
        }

        if (valueLength < -1) {
            return WalDecodeResult.Corrupted(
                "Invalid value length: $valueLength"
            )
        }

        val payloadSize =
            keyLength.toLong() +
                maxOf(valueLength, 0).toLong()

        val recordSize =
            HEADER_SIZE.toLong() + payloadSize

        if (recordSize > Int.MAX_VALUE) {
            return WalDecodeResult.Corrupted(
                "WAL record too large"
            )
        }

        if (data.size - offset < recordSize) {
            return WalDecodeResult.Incomplete
        }

        val keyStart = offset + HEADER_SIZE
        val keyEnd = keyStart + keyLength

        val key = data.copyOfRange(
            keyStart,
            keyEnd,
        )

        val value =
            if (valueLength == -1) {
                null
            } else {
                data.copyOfRange(
                    keyEnd,
                    keyEnd + valueLength,
                )
            }

        return WalDecodeResult.Success(
            record = WalRecord(
                sequence = sequence,
                operation = operation,
                key = key,
                value = value,
            ),
            bytesRead = recordSize.toInt(),
        )
    }


    private fun readInt(
        buffer: ByteArray,
        offset: Int,
    ): Int {
        return ((buffer[offset].toInt() and 0xff) shl 24) or
            ((buffer[offset + 1].toInt() and 0xff) shl 16) or
            ((buffer[offset + 2].toInt() and 0xff) shl 8) or
            (buffer[offset + 3].toInt() and 0xff)
    }

    private fun readLong(
        buffer: ByteArray,
        offset: Int,
    ): Long {
        var result = 0L

        for (i in 0 until 8) {
            result =
                (result shl 8) or
                    (buffer[offset + i].toLong() and 0xffL)
        }

        return result
    }
}