package net.kigawa.fortis.storage.engine.wal

import net.kigawa.fortis.storage.engine.wal.codec.WalRecord

interface Wal {
    suspend fun append(record: WalRecord)
    suspend fun replay(
        handler: suspend (WalRecord) -> Unit,
    )
    suspend fun sync()
}