package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
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
    proposalCoroutineContext: CoroutineContext = Dispatchers.Default,
) {
    private val mutex = Mutex()
    private val peerRpcMutexes = initialNode.peerIds.associateWith { Mutex() }
    private var node: RaftNode = initialNode
    private val appendAttempts = mutableMapOf<String, AppendAttempt>()
    private val proposalJob = SupervisorJob(proposalCoroutineContext[Job])
    private val proposalScope = CoroutineScope(proposalCoroutineContext + proposalJob)
    private val proposals = mutableMapOf<Long, PendingProposal>()

    val currentNode: RaftNode
        get() = node

    suspend fun start() {
        node.timer.reset(RaftTimeoutEvent.Election)
    }

    suspend fun stop() {
        proposalJob.cancel()
        mutex.withLock {
            proposals.values.forEach { it.completion.completeExceptionally(RaftProposalStoppedException()) }
            proposals.clear()
        }
        node.timer.cancel()
    }

    suspend fun handleRequestVote(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        val result = node.handleRequestVote(request)
        node = result.node
        settleProposals()
        result.value
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        val result = node.handleAppendEntries(request)
        node = result.node
        settleProposals()
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
            settleProposals()
            val candidate = node as? CandidateNode ?: return
            ElectionRound(candidate, result.value, node.peerIds.toList())
        }

        coroutineScope {
            for (peerId in election.peerIds) {
                launch { requestVote(election, peerId) }
            }
        }
    }

    private suspend fun requestVote(election: ElectionRound, peerId: String) {
        peerRpcMutexes.getValue(peerId).withLock peerLock@{
            if (!mutex.withLock { isCurrentElection(election) }) return@peerLock
            val response = try {
                transport.requestVote(peerId, election.request)
            } catch (_: RaftTransportException) {
                return@peerLock
            }
            mutex.withLock stateLock@{
                if (observeHigherTerm(response.term)) return@stateLock
                if (!isCurrentElection(election)) return@stateLock
                node = election.candidate.handleRequestVoteResponse(peerId, response)
                settleProposals()
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

    /** Local append and replication attempt; use [propose] for a committed and applied write. */
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

    /**
     * Returns only after majority commit and local apply of this index and term.
     * Timeout/cancellation ends the wait, without rolling back an accepted log entry;
     * its outcome is unknown and replication may still commit it later.
     * Leadership loss fails a pending proposal, also without rolling back its entry.
     * [stop] fails pending proposals and prevents new proposal submissions.
     */
    suspend fun propose(command: RaftCommand, timeout: Duration = 5.seconds): RaftLogEntry {
        require(timeout.isPositive() && timeout.isFinite()) { "Proposal timeout must be positive and finite" }
        return withTimeout(timeout) {
            val completion = CompletableDeferred<RaftLogEntry>()
            val waiterJob = currentCoroutineContext()[Job]
            val submission = proposalScope.launch {
                try {
                    val round = mutex.withLock {
                        if (!completion.isActive || waiterJob?.isActive == false) return@launch
                        if (!proposalJob.isActive) throw RaftProposalStoppedException()
                        val leader = node as? LeaderNode ?: throw RaftProposalNotLeaderException()
                        val entry = leader.appendCommand(command)
                        val round = leaderRound(leader)
                        if (completion.isActive) proposals[entry.index] = PendingProposal(round, entry, completion)
                        settleProposals()
                        round
                    }
                    replicateRound(round)
                } catch (cause: Throwable) {
                    completion.completeExceptionally(
                        if (!proposalJob.isActive) RaftProposalStoppedException() else cause,
                    )
                }
            }
            submission.invokeOnCompletion { cause ->
                if (cause != null) completion.completeExceptionally(RaftProposalStoppedException())
            }
            try {
                completion.await()
            } finally {
                completion.cancel()
                // Cleanup must not delay a timeout while a state machine is applying.
                proposalScope.launch {
                    mutex.withLock {
                        proposals.entries.removeAll { it.value.completion === completion }
                    }
                }
            }
        }
    }

    suspend fun replicate() {
        val round = mutex.withLock {
            val leader = node as? LeaderNode ?: return
            leaderRound(leader)
        }
        replicateRound(round)
    }

    private suspend fun replicateRound(round: LeaderRound) = coroutineScope {
        for (peerId in round.peerIds) {
            launch {
                peerRpcMutexes.getValue(peerId).withLock {
                    replicatePeer(round, peerId)
                }
            }
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
                settleProposals()
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
        settleProposals()
        node.timer.reset(RaftTimeoutEvent.Election)
        return true
    }

    private fun settleProposals() {
        val iterator = proposals.values.iterator()
        while (iterator.hasNext()) {
            val proposal = iterator.next()
            when {
                !proposal.completion.isActive -> iterator.remove()
                !proposalJob.isActive -> {
                    proposal.completion.completeExceptionally(RaftProposalStoppedException())
                    iterator.remove()
                }
                !isCurrentLeader(proposal.round) -> {
                    proposal.completion.completeExceptionally(
                        RaftProposalLeadershipLostException(proposal.entry.index, proposal.entry.term),
                    )
                    iterator.remove()
                }
                node.volatileState.commitIndex >= proposal.entry.index &&
                    node.volatileState.lastApplied >= proposal.entry.index -> {
                    proposal.completion.complete(proposal.entry)
                    iterator.remove()
                }
            }
        }
    }

    private data class PendingProposal(
        val round: LeaderRound,
        val entry: RaftLogEntry,
        val completion: CompletableDeferred<RaftLogEntry>,
    )

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
