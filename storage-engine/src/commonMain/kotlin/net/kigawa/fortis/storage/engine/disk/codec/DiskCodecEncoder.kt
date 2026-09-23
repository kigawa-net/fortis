package net.kigawa.fortis.storage.engine.disk.codec

data class DiskCodecEncoder(
    val headerSize: Int,
    val version: Byte,
    val put: Byte,
    val delete: Byte,
) {
    fun encodePut(
        key: ByteArray,
        value: ByteArray,
    ): ByteArray =
        encode(
            operation = put,
            key = key,
            value = value,
        )

    fun encodeDelete(
        key: ByteArray,
    ): ByteArray =
        encode(
            operation = delete,
            key = key,
            value = null,
        )

    private fun encode(
        operation: Byte,
        key: ByteArray,
        value: ByteArray?,
    ): ByteArray {
        val valueLength = value?.size ?: -1

        val result = ByteArray(
            headerSize +
                key.size +
                maxOf(valueLength, 0)
        )

        result[0] = 'F'.code.toByte()
        result[1] = 'D'.code.toByte()
        result[2] = 'A'.code.toByte()
        result[3] = 'T'.code.toByte()

        result[4] = version
        result[5] = operation

        writeInt(
            result,
            6,
            key.size,
        )

        writeInt(
            result,
            10,
            valueLength,
        )

        key.copyInto(
            result,
            destinationOffset = headerSize,
        )

        value?.copyInto(
            result,
            destinationOffset =
                headerSize + key.size,
        )

        return result
    }

    private fun writeInt(
        data: ByteArray,
        offset: Int,
        value: Int,
    ) {
        data[offset] = (value ushr 24).toByte()
        data[offset + 1] = (value ushr 16).toByte()
        data[offset + 2] = (value ushr 8).toByte()
        data[offset + 3] = value.toByte()
    }

}