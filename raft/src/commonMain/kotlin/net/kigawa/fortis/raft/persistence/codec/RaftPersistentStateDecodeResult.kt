package net.kigawa.fortis.raft.persistence.codec

import net.kigawa.fortis.raft.RaftPersistentState

sealed interface RaftPersistentStateDecodeResult {
    data class Success(
        val state: RaftPersistentState,
    ) : RaftPersistentStateDecodeResult

    data object Incomplete : RaftPersistentStateDecodeResult

    data class Corrupted(
        val reason: String,
    ) : RaftPersistentStateDecodeResult
}
