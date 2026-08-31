package net.kigawa.fortis.storage.engine.wal

data class DecodedWalRecord(
    val record: WalRecord,
    val bytesRead: Int,
)