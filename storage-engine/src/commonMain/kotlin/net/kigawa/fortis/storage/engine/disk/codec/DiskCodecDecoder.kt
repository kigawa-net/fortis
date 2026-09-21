package net.kigawa.fortis.storage.engine.disk.codec

data class DiskCodecDecoder(
    val headerSize: Int,
    val version: Byte,
    val put: Byte,
    val delete: Byte,
) {
    fun decodeHeader(
        data: ByteArray,
    ): DiskRecordHeader {
        require(data.size >= headerSize)

        require(
            data[0] == 'F'.code.toByte() &&
                data[1] == 'D'.code.toByte() &&
                data[2] == 'A'.code.toByte() &&
                data[3] == 'T'.code.toByte()
        ) {
            "Invalid disk record magic"
        }

        require(data[4] == version) {
            "Unsupported disk record version: ${data[4]}"
        }

        val operation =
            when (data[5]) {
                put -> DiskOperation.Put
                delete -> DiskOperation.Delete
                else -> throw IllegalArgumentException(
                    "Unknown disk operation: ${data[5]}"
                )
            }

        val keyLength = readInt(data, 6)
        val valueLength = readInt(data, 10)

        require(keyLength >= 0)

        operation.validateValueLength(valueLength)

        return DiskRecordHeader(
            operation = operation,
            keyLength = keyLength,
            valueLength = valueLength,
        )
    }

    private fun readInt(
        data: ByteArray,
        offset: Int,
    ): Int =
        ((data[offset].toInt() and 0xff) shl 24) or
            ((data[offset + 1].toInt() and 0xff) shl 16) or
            ((data[offset + 2].toInt() and 0xff) shl 8) or
            (data[offset + 3].toInt() and 0xff)
}