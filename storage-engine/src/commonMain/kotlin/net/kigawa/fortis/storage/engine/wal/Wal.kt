package net.kigawa.fortis.storage.engine.wal

interface Wal {
    suspend fun append(record: WalRecord)
    suspend fun replay(
        handler: suspend (WalRecord) -> Unit,
    )
    suspend fun sync()
}