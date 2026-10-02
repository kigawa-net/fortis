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
    private val appendAttempts = mutableMapOf<String, AppendAttempt>()

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

    suspend fun onElectionTimeout() {
        val election = mutex.withLock {
            val result = when (val current = node) {
                is FollowerNode -> current.onElectionTimeout()
                is CandidateNode -> current.onElectionTimeout()
                else -> return
            }
            node = result.node
            val candidate = node as? CandidateNode ?: return
            ElectionRound(candidate, result.value, node.peerIds.toList())
        }

        for (peerId in election.peerIds) {
            if (!mutex.withLock { isCurrentElection(election) }) return
            val response = try {
                transport.requestVote(peerId, election.request)
            } catch (_: RaftTransportException) {
                continue
            }
            mutex.withLock {
                if (observeHigherTerm(response.term)) return@withLock
                if (!isCurrentElection(election)) return@withLock
                node = election.candidate.handleRequestVoteResponse(peerId, response)
            }
        }
    }

    suspend fun onHeartbeatTimeout() {
        val round = mutex.withLock {
            val leader = node as? LeaderNode ?: return
            leader.timer.reset(RaftTimeoutEvent.Heartbeat)
            leaderRound(leader)
        }
        replicateRound(round)
    }

    suspend fun appendCommand(
        command: RaftCommand,
    ): RaftLogEntry {
        val (entry, round) = mutex.withLock {
            val leader = node as? LeaderNode
                ?: error("Only leader can append commands")
            leader.appendCommand(command) to leaderRound(leader)
        }
        replicateRound(round)
        return entry
    }

    suspend fun replicate() {
        val round = mutex.withLock {
            val leader = node as? LeaderNode ?: return
            leaderRound(leader)
        }
        replicateRound(round)
    }

    private suspend fun replicateRound(round: LeaderRound) {
        for (peerId in round.peerIds) {
            replicatePeer(round, peerId)
        }
    }

    private suspend fun replicatePeer(
        round: LeaderRound,
        peerId: String,
    ) {
        while (true) {
            val attempt = mutex.withLock {
                if (!isCurrentLeader(round)) return
                AppendAttempt(round, round.leader.createAppendEntries(peerId)).also {
                    appendAttempts[peerId] = it
                }
            }
            val response = try {
                transport.appendEntries(peerId, attempt.request)
            } catch (_: RaftTransportException) {
                return
            }
            val retry = mutex.withLock {
                if (observeHigherTerm(response.term)) return@withLock false
                if (
                    !isCurrentLeader(attempt.round) ||
                    appendAttempts[peerId] !== attempt ||
                    response.term != attempt.request.term
                ) {
                    return@withLock false
                }
                appendAttempts.remove(peerId)
                node = round.leader.handleAppendEntriesResponse(peerId, attempt.request, response)
                !response.success && isCurrentLeader(round)
            }
            if (!retry) return
        }
    }

    private fun isCurrentElection(round: ElectionRound): Boolean =
        node === round.candidate && node.persistentState.currentTerm == round.request.term

    private fun leaderRound(leader: LeaderNode) = LeaderRound(
        leader,
        leader.persistentState.currentTerm,
        leader.peerIds.toList(),
    )

    private fun isCurrentLeader(round: LeaderRound): Boolean =
        node === round.leader && node.persistentState.currentTerm == round.term

    // Higher terms remain authoritative even when the original RPC is stale.
    private suspend fun observeHigherTerm(term: Long): Boolean {
        if (term <= node.persistentState.currentTerm) return false
        node.persistentStateStore.save(term, null)
        node.persistentState.currentTerm = term
        node.persistentState.votedFor = null
        node = node.follower()
        appendAttempts.clear()
        node.timer.reset(RaftTimeoutEvent.Election)
        return true
    }

    private data class ElectionRound(
        val candidate: CandidateNode,
        val request: RequestVoteRequest,
        val peerIds: List<String>,
    )

    private data class LeaderRound(
        val leader: LeaderNode,
        val term: Long,
        val peerIds: List<String>,
    )

    private class AppendAttempt(
        val round: LeaderRound,
        val request: AppendEntriesRequest,
    )
}
