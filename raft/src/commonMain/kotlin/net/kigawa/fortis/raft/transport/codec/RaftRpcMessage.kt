package net.kigawa.fortis.raft.transport.codec

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

sealed interface RaftRpcMessage {
    data class RequestVote(
        val request: RequestVoteRequest,
    ) : RaftRpcMessage

    data class RequestVoteResult(
        val response: RequestVoteResponse,
    ) : RaftRpcMessage

    data class AppendEntries(
        val request: AppendEntriesRequest,
    ) : RaftRpcMessage

    data class AppendEntriesResult(
        val response: AppendEntriesResponse,
    ) : RaftRpcMessage
}
