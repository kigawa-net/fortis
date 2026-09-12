package net.kigawa.fortis.storage.engine.wal

data class WalCodec(
    val headerSize: Int = 22,
    val version: Byte = 1,
) {
    fun encode(record: WalRecord): ByteArray = WalEncoder(record, this).encode()
    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): WalDecodeResult = WalDecoder(data, offset, this).decode()
}
