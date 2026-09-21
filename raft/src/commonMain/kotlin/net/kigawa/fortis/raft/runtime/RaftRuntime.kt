package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.transport.RaftTransport

class RaftRuntime(
    private val node: RaftNode,
    private val transport: RaftTransport,
    private val peerIds: Set<String>,
) {
    private val mutex = Mutex()

    suspend fun onElectionTimeout(): Unit = mutex.withLock {
        val request = node.onElectionTimeout()
        for (peerId in peerIds) {
            val response = transport.requestVote(peerId, request)
            node.handleRequestVoteResponse(peerId, response)
        }
    }

    suspend fun onHeartbeatTimeout(): Unit = mutex.withLock {
        if (node !is LeaderNode) {
            throw IllegalStateException("Only leader handles heartbeat timeout")
        }
        val requests = node.onHeartbeatTimeout()
        for ((peerId, request) in requests) {
            replicatePeer(peerId, request)
        }
    }

    suspend fun appendCommand(
        command: RaftCommand,
    ): RaftLogEntry = mutex.withLock {
        val entry = node.appendCommand(command)
        replicateLocked()
        entry
    }

    suspend fun replicate(): Unit = mutex.withLock {
        replicateLocked()
    }

    private suspend fun replicateLocked() {
        for (peerId in peerIds) {
            if (node.role != RaftRole.LEADER) {
                return
            }
            replicatePeer(peerId)
        }
    }

    private suspend fun replicatePeer(
        peerId: String,
        initialRequest: AppendEntriesRequest? = null,
    ) {
        if (node !is LeaderNode) {
            throw IllegalStateException("Only leader handles replicate")
        }
        var request = initialRequest
        while (node.role == RaftRole.LEADER) {
            val currentRequest = request ?: node.createAppendEntries(peerId)
            val response = transport.appendEntries(peerId, currentRequest)
            node.handleAppendEntriesResponse(peerId, currentRequest, response)
            if (response.success || response.term > currentRequest.term) {
                return
            }
            request = null
        }
    }
}
