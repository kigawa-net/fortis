package net.kigawa.fortis.raft.log.codec

import net.kigawa.fortis.raft.log.RaftLogEntry

sealed interface RaftLogDecodeResult {
    data class Success(
        val entry: RaftLogEntry,
        val bytesRead: Int,
    ) : RaftLogDecodeResult

    data object Incomplete : RaftLogDecodeResult

    data class Corrupted(
        val reason: String,
    ) : RaftLogDecodeResult
}
