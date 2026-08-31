package net.kigawa.fortis.storage.engine.wal

object WalCodec {
    fun encode(record: WalRecord): ByteArray {
        TODO()
    }

    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): DecodedWalRecord {
        TODO()
    }
}