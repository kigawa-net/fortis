package net.kigawa.fortis.raft.transport.codec

import net.kigawa.fortis.raft.transport.RaftTransportException

/** Raft RPC の要求・応答に担当グループを付与する。 */
data class GroupRaftRpcMessage(val groupId: String, val message: RaftRpcMessage) {
    init {
        require(groupId.isNotEmpty()) { "Raft group ID must not be empty" }
    }
}

internal fun RaftRpcMessage.senderId(): String = when (this) {
    is RaftRpcMessage.RequestVote -> request.candidateId
    is RaftRpcMessage.AppendEntries -> request.leaderId
    is RaftRpcMessage.SnapshotChunk -> chunk.leaderId
    is RaftRpcMessage.InstallSnapshot -> request.leaderId
    else -> throw RaftTransportException("Raft RPC server received a response message")
}
