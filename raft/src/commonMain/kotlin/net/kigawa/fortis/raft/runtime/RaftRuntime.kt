package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RaftRuntime(
    initialNode: RaftNode,
    private val transport: RaftTransport,
) {
    private val mutex = Mutex()
    private var node: RaftNode = initialNode

    val currentNode: RaftNode
        get() = node

    suspend fun start() {
        node.timer.reset(RaftTimeoutEvent.Election)
    }

    suspend fun stop() {
        node.timer.cancel()
    }

    suspend fun handleRequestVote(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        val result = node.handleRequestVote(request)
        node = result.node
        result.value
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        val result = node.handleAppendEntries(request)
        node = result.node
        result.value
    }

    suspend fun onElectionTimeout(): Unit = mutex.withLock {
        val result = when (val current = node) {
            is FollowerNode -> current.onElectionTimeout()
            is CandidateNode -> current.onElectionTimeout()
            is LeaderNode -> return@withLock
            else -> return@withLock
        }
        node = result.node

        for (peerId in node.peerIds) {
            val candidate = node as? CandidateNode ?: break
            val response = try {
                transport.requestVote(peerId, result.value)
            } catch (_: RaftTransportException) {
                continue
            }
            node = candidate.handleRequestVoteResponse(peerId, response)
        }
    }

    suspend fun onHeartbeatTimeout(): Unit = mutex.withLock {
        val leader = node as? LeaderNode ?: return@withLock
        val requests = leader.onHeartbeatTimeout()
        for ((peerId, request) in requests) {
            replicatePeer(peerId, request)
        }
    }

    suspend fun appendCommand(
        command: RaftCommand,
    ): RaftLogEntry = mutex.withLock {
        val leader = node as? LeaderNode
            ?: error("Only leader can append commands")
        val entry = leader.appendCommand(command)
        replicateLocked()
        entry
    }

    suspend fun replicate(): Unit = mutex.withLock {
        replicateLocked()
    }

    private suspend fun replicateLocked() {
        for (peerId in node.peerIds) {
            if (node !is LeaderNode) return
            replicatePeer(peerId)
        }
    }

    private suspend fun replicatePeer(
        peerId: String,
        initialRequest: AppendEntriesRequest? = null,
    ) {
        var request = initialRequest
        while (true) {
            val leader = node as? LeaderNode ?: return
            val currentRequest = request ?: leader.createAppendEntries(peerId)
            val response = try {
                transport.appendEntries(peerId, currentRequest)
            } catch (_: RaftTransportException) {
                return
            }
            node = leader.handleAppendEntriesResponse(
                peerId,
                currentRequest,
                response,
            )
            if (response.success || node !is LeaderNode) return
            request = null
        }
    }
}
