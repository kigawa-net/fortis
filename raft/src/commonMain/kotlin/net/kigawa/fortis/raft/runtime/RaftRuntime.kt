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
import net.kigawa.fortis.raft.RaftStateMachine
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
    private val reads = mutableSetOf<ReadIndexRequestContext>()

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
            reads.forEach { it.completion.completeExceptionally(RaftReadStoppedException()) }
            reads.clear()
        }
        node.timer.cancel()
    }

    suspend fun handleRequestVote(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        val result = node.handleRequestVote(request)
        node = result.node
        settlePending()
        result.value
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        val result = node.handleAppendEntries(request)
        node = result.node
        settlePending()
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
            settlePending()
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
                settlePending()
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
                        settlePending()
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

    /**
     * Confirms leadership using fresh, request-correlated RPCs and waits for local apply.
     * A current-term commit is required first; replication establishes it when necessary.
     * This is a barrier, not a reusable lease. Use [linearizableRead] to read local state.
     * Timeout/cancellation ends the wait and cancels its confirmation RPCs.
     */
    suspend fun readIndex(timeout: Duration = 5.seconds): RaftReadIndex {
        require(timeout.isPositive() && timeout.isFinite()) { "Read timeout must be positive and finite" }
        return withTimeout(timeout) {
            val completion = CompletableDeferred<RaftReadIndex>()
            val waiterJob = currentCoroutineContext()[Job]
            val probeJob = SupervisorJob(proposalJob)
            val submission = proposalScope.launch {
                try {
                    val initialReplication = mutex.withLock {
                        if (!completion.isActive || waiterJob?.isActive == false) return@launch
                        if (!proposalJob.isActive) throw RaftReadStoppedException()
                        val leader = node as? LeaderNode ?: throw RaftReadNotLeaderException()
                        val round = leaderRound(leader)
                        reads.add(ReadIndexRequestContext(round, completion, probeJob))
                        settleReads()
                        if (leader.isReady) null else round
                    }
                    // This belongs to the runtime: cancelling a read must not interrupt apply.
                    if (initialReplication != null) replicateRound(initialReplication)
                } catch (cause: Throwable) {
                    completion.completeExceptionally(
                        if (!proposalJob.isActive) RaftReadStoppedException() else cause,
                    )
                }
            }
            submission.invokeOnCompletion { cause ->
                if (cause != null) completion.completeExceptionally(RaftReadStoppedException())
            }
            try {
                completion.await()
            } finally {
                completion.cancel()
                probeJob.cancel()
                proposalScope.launch {
                    mutex.withLock { reads.removeAll { it.completion === completion } }
                }
            }
        }
    }

    /**
     * Reads applied state after a fresh quorum check, rechecking role/term under the state lock.
     * The callback runs under that lock and must only read state, without re-entering this runtime.
     * The timeout covers readiness, quorum confirmation, apply, and the callback.
     */
    suspend fun <T> linearizableRead(
        timeout: Duration = 5.seconds,
        read: suspend (RaftStateMachine) -> T,
    ): T {
        require(timeout.isPositive() && timeout.isFinite()) { "Read timeout must be positive and finite" }
        return withTimeout(timeout) {
            val barrier = readIndex(timeout)
            mutex.withLock {
                if (!proposalJob.isActive) throw RaftReadStoppedException()
                val leader = node as? LeaderNode ?: throw RaftReadLeadershipLostException(barrier.term)
                if (leader.persistentState.currentTerm != barrier.term) {
                    throw RaftReadLeadershipLostException(barrier.term)
                }
                check(leader.volatileState.lastApplied >= barrier.index) { "Read barrier is not applied" }
                val result = read(leader.stateMachine)
                if (!proposalJob.isActive) throw RaftReadStoppedException()
                result
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
                settlePending()
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
        settlePending()
        node.timer.reset(RaftTimeoutEvent.Election)
        return true
    }

    private fun settlePending() {
        settleProposals()
        settleReads()
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

    private fun settleReads() {
        val iterator = reads.iterator()
        while (iterator.hasNext()) {
            val read = iterator.next()
            when {
                !read.completion.isActive -> iterator.remove()
                !proposalJob.isActive -> {
                    read.completion.completeExceptionally(RaftReadStoppedException())
                    iterator.remove()
                }
                !isCurrentLeader(read.round) -> {
                    read.completion.completeExceptionally(RaftReadLeadershipLostException(read.round.term))
                    iterator.remove()
                }
                else -> {
                    if (read.index == null && read.round.leader.isReady) {
                        // Snapshot before sending probes; older RPCs cannot satisfy this context.
                        read.index = node.volatileState.commitIndex
                        val scope = CoroutineScope(proposalScope.coroutineContext + read.probeJob)
                        for (peerId in read.round.peerIds) {
                            scope.launch {
                                try {
                                    confirmRead(read, peerId)
                                } catch (cause: Throwable) {
                                    if (read.probeJob.isActive) read.completion.completeExceptionally(cause)
                                }
                            }
                        }
                    }
                    val index = read.index
                    if (index != null &&
                        read.confirmedPeers.size >= (read.round.peerIds.size + 1) / 2 + 1 &&
                        node.volatileState.lastApplied >= index
                    ) {
                        read.completion.complete(RaftReadIndex(index, read.round.term))
                        iterator.remove()
                    }
                }
            }
        }
    }

    private suspend fun confirmRead(read: ReadIndexRequestContext, peerId: String) {
        peerRpcMutexes.getValue(peerId).withLock peerLock@{
            val request = mutex.withLock {
                if (!read.completion.isActive || !isCurrentLeader(read.round)) return@peerLock
                // A term-only heartbeat: never changes peer progress, logs, or commitIndex.
                // The RPC's response is correlated to this context by the transport call/stream.
                AppendEntriesRequest(read.round.term, node.nodeId, 0, 0, emptyList(), 0)
            }
            val response = try {
                transport.appendEntries(peerId, request)
            } catch (_: RaftTransportException) {
                return@peerLock
            }
            mutex.withLock stateLock@{
                if (observeHigherTerm(response.term)) return@stateLock
                if (!read.completion.isActive || !isCurrentLeader(read.round)) return@stateLock
                if (response.term == read.round.term && response.success) {
                    read.confirmedPeers.add(peerId)
                    settleReads()
                }
            }
        }
    }

    private class ReadIndexRequestContext(
        val round: LeaderRound,
        val completion: CompletableDeferred<RaftReadIndex>,
        val probeJob: Job,
    ) {
        var index: Long? = null
        val confirmedPeers = mutableSetOf(round.leader.nodeId)
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
