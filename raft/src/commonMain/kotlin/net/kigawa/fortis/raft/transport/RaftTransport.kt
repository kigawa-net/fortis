package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

/** Each result must belong to that call's request; responses must never be cached or reused. */
interface RaftTransport {
    suspend fun requestVote(
        peerId: String,
        request: RequestVoteRequest,
    ): RequestVoteResponse

    suspend fun appendEntries(
        peerId: String,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse
}
