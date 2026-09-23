package net.kigawa.fortis.raft.transport.codec

sealed interface RaftRpcDecodeResult {
    data class Success(
        val message: RaftRpcMessage,
        val bytesRead: Int,
    ) : RaftRpcDecodeResult

    data object Incomplete : RaftRpcDecodeResult

    data class Corrupted(
        val reason: String,
    ) : RaftRpcDecodeResult
}
