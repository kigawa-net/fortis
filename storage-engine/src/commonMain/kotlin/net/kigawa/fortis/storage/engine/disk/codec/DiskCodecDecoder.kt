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
        validateFirst(data)
        val operation = setOf(DiskOperation.Delete, DiskOperation.Put)
            .find { it.code == data[5] }
            ?: throw IllegalArgumentException(
                "Unknown disk operation: ${data[5]}"
            )
        val reader = DiskCodecReader(data)
        val keyLength = reader.readInt(6)
        val valueLength = reader.readInt(10)

        require(keyLength >= 0)

        operation.validateValueLength(valueLength)

        return DiskRecordHeader(
            operation = operation,
            keyLength = keyLength,
            valueLength = valueLength,
        )
    }
    fun validateFirst(data: ByteArray) {
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
    }
}