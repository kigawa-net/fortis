package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class LocalRaftTransport : RaftTransport {
    private val runtimes = mutableMapOf<String, RaftRuntime>()

    fun register(nodeId: String, runtime: RaftRuntime) {
        require(nodeId !in runtimes) { "Node already registered: $nodeId" }
        runtimes[nodeId] = runtime
    }

    override suspend fun requestVote(
        peerId: String,
        request: RequestVoteRequest,
    ): RequestVoteResponse = runtime(peerId).handleRequestVote(request)

    override suspend fun appendEntries(
        peerId: String,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = runtime(peerId).handleAppendEntries(request)

    private fun runtime(peerId: String): RaftRuntime =
        runtimes[peerId] ?: throw RaftPeerUnavailableException(peerId)
}
