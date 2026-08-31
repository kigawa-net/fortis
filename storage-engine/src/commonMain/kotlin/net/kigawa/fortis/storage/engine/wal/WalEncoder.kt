package net.kigawa.fortis.storage.engine.wal

class WalEncoder(
    val record: WalRecord,
) {
    val valueLength = record.value?.size ?: -1
    val totalSize =
        WalCodec.HEADER_SIZE +
            record.key.size +
            maxOf(valueLength, 0)
    val buffer = ByteArray(totalSize)
    fun encode(): ByteArray {require(
        when (record.operation) {
            WalOperation.PUT -> record.value != null
            WalOperation.DELETE -> record.value == null
        }
    )
        // magic
        buffer[0] = 'F'.code.toByte()
        buffer[1] = 'W'.code.toByte()
        buffer[2] = 'A'.code.toByte()
        buffer[3] = 'L'.code.toByte()

        buffer[4] = WalCodec.VERSION
        buffer[5] = record.operation.code

        writeLong(
            offset = 6,
            value = record.sequence,
        )

        writeInt(
            offset = 14,
            value = record.key.size,
        )

        writeInt(
            offset = 18,
            value = valueLength,
        )

        record.key.copyInto(
            destination = buffer,
            destinationOffset = WalCodec.HEADER_SIZE,
        )

        record.value?.copyInto(
            destination = buffer,
            destinationOffset = WalCodec.HEADER_SIZE + record.key.size,
        )

        return buffer
    }

    private fun writeInt(
        offset: Int,
        value: Int,
    ) {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
    }

    private fun writeLong(
        offset: Int,
        value: Long,
    ) {
        for (i in 0 until 8) {
            buffer[offset + i] =
                (value ushr (56 - i * 8)).toByte()
        }
    }

}