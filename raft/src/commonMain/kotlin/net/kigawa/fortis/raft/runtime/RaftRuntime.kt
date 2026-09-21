package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftRole
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
        val requests = node.onHeartbeatTimeout()
        for ((peerId, request) in requests) {
            val response = transport.appendEntries(peerId, request)
            node.handleAppendEntriesResponse(peerId, request, response)
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
            val request = node.createAppendEntries(peerId)
            val response = transport.appendEntries(peerId, request)
            node.handleAppendEntriesResponse(peerId, request, response)
        }
    }
}
