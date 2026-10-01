package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage

class RaftRpcHandler(
    private val runtime: RaftRuntime,
) {
    private val peerIds = runtime.currentNode.peerIds.toSet()

    fun isKnownPeer(peerId: String): Boolean = peerId in peerIds

    suspend fun handle(message: RaftRpcMessage): RaftRpcMessage = when (message) {
        is RaftRpcMessage.RequestVote -> RaftRpcMessage.RequestVoteResult(
            runtime.handleRequestVote(message.request),
        )

        is RaftRpcMessage.AppendEntries -> RaftRpcMessage.AppendEntriesResult(
            runtime.handleAppendEntries(message.request),
        )

        is RaftRpcMessage.RequestVoteResult,
        is RaftRpcMessage.AppendEntriesResult,
        -> throw RaftTransportException(
            "Unexpected RPC request: ${message::class.simpleName}",
        )
    }
}
