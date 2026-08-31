package net.kigawa.fortis.storage.engine.wal

object WalCodec {
    internal const val HEADER_SIZE = 22
    internal const val VERSION: Byte = 1
    fun encode(record: WalRecord): ByteArray = WalEncoder(record).encode()
    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): WalDecodeResult = WalDecoder(data, offset).decode()
}