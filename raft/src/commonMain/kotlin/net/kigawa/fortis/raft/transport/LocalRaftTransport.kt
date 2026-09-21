package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class LocalRaftTransport(
    private val nodes: Map<String, RaftNode>,
) : RaftTransport {
    override suspend fun requestVote(
        peerId: String,
        request: RequestVoteRequest,
    ): RequestVoteResponse = node(peerId).handleRequestVote(request)

    override suspend fun appendEntries(
        peerId: String,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = node(peerId).handleAppendEntries(request)

    private fun node(peerId: String): RaftNode =
        requireNotNull(nodes[peerId]) {
            "Unknown peer: $peerId"
        }
}
