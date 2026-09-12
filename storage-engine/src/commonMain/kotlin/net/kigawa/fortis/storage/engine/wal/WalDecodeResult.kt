package net.kigawa.fortis.storage.engine.wal

sealed interface WalDecodeResult {
    data class Success(
        val record: WalRecord,
        val bytesRead: Int,
    ) : WalDecodeResult

    data object Incomplete : WalDecodeResult

    data class Corrupted(
        val reason: String,
    ) : WalDecodeResult
}