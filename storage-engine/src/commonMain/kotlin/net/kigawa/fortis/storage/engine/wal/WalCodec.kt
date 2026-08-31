package net.kigawa.fortis.storage.engine.wal

object WalCodec {
    private const val HEADER_SIZE = 22
    private const val VERSION: Byte = 1

    fun encode(record: WalRecord): ByteArray {
        TODO()
    }

    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): WalDecodeResult {
        TODO()
    }
    private fun writeInt(
        buffer: ByteArray,
        offset: Int,
        value: Int,
    ) {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
    }
}