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
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.logging.NoOpLogger
import net.kigawa.fortis.raft.logging.RaftLogEvents
import net.kigawa.fortis.raft.logging.RaftLogger
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotHandler
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.snapshot.RaftSnapshotProvider
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.snapshot.FileRaftSnapshotStore
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunk
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunkResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunks
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.vote.RaftVotingConfiguration
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.coroutines.cancellation.CancellationException

class RaftRuntime(
    initialNode: RaftNode,
    private val transport: RaftTransport,
    proposalCoroutineContext: CoroutineContext = Dispatchers.Default,
    private val snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
    private val snapshotProvider: RaftSnapshotProvider? = null,
    private val logger: RaftLogger,
    private val snapshotStore: FileRaftSnapshotStore? = null,
    private val snapshotChunkBytes: Int? = null,
) {
    /** 既存のコンストラクタを維持し、ログ出力を無効にして生成する。 */
    constructor(
        initialNode: RaftNode,
        transport: RaftTransport,
        proposalCoroutineContext: CoroutineContext = Dispatchers.Default,
        snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
        snapshotProvider: RaftSnapshotProvider? = null,
    ) : this(initialNode, transport, proposalCoroutineContext, snapshotApplier, snapshotProvider, NoOpLogger)

    /** 永続化と分割転送を設定して生成する。 */
    constructor(
        initialNode: RaftNode,
        transport: RaftTransport,
        snapshotStore: FileRaftSnapshotStore?,
        snapshotChunkBytes: Int? = null,
        proposalCoroutineContext: CoroutineContext = Dispatchers.Default,
        snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
        snapshotProvider: RaftSnapshotProvider? = null,
    ) : this(initialNode, transport, proposalCoroutineContext, snapshotApplier, snapshotProvider, NoOpLogger,
        snapshotStore, snapshotChunkBytes)

    init { require(snapshotChunkBytes == null || snapshotChunkBytes > 0) }

    private val mutex = Mutex()
    private val peerRpcMutexes = mutableMapOf<String, Mutex>()

    private suspend fun peerRpcMutex(peerId: String): Mutex =
        mutex.withLock { peerRpcMutexes.getOrPut(peerId) { Mutex() } }
    private var node: RaftNode = initialNode
    private var leaderHint: LeaderHint? = null
    private val appendAttempts = mutableMapOf<String, AppendAttempt>()
    private val snapshotAttempts = mutableMapOf<String, SnapshotAttempt>()
    private val snapshotHandler = InstallSnapshotHandler(
        initialNode.persistentState,
        initialNode.persistentStateStore,
        initialNode.volatileState,
        initialNode.log,
        initialNode.timer,
        snapshotApplier,
        initialLastIncludedIndex = initialNode.volatileState.lastApplied,
        snapshotStore = snapshotStore,
    )
    private val proposalJob = SupervisorJob(proposalCoroutineContext[Job])
    private val proposalScope = CoroutineScope(proposalCoroutineContext + proposalJob)
    private val proposals = mutableMapOf<Long, PendingProposal>()
    private val reads = mutableSetOf<ReadIndexRequestContext>()

    val currentNode: RaftNode
        get() = node

    /** 現在の任期で確認したリーダーの ID を返す。 */
    suspend fun knownLeaderId(): String? = mutex.withLock {
        if (!proposalJob.isActive) return@withLock null
        when (val current = node) {
            is LeaderNode -> current.nodeId
            is FollowerNode -> leaderHint?.takeIf { it.term == current.persistentState.currentTerm }?.peerId
            else -> null
        }
    }

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
        val previous = node
        val result = node.handleRequestVote(request)
        node = result.node
        logLeadershipTransition(previous, node)
        settlePending()
        result.value
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        val previous = node
        val result = node.handleAppendEntries(request)
        node = result.node
        rememberLeader(request.term, request.leaderId)
        logLeadershipTransition(previous, node)
        settlePending()
        result.value
    }

    /** 単一メッセージのInstallSnapshotを受け付け、有効な任期なら降格する。 */
    suspend fun handleInstallSnapshot(
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse = mutex.withLock {
        val previous = node
        val previousTerm = node.persistentState.currentTerm
        val response = snapshotHandler.handle(request)
        logger.info(
            RaftLogEvents.COMPONENT_RUNTIME,
            RaftLogEvents.INSTALL_SNAPSHOT_RECEIVED,
            mapOf(
                "term" to request.term.toString(),
                "leaderId" to request.leaderId,
            ),
        )
        if (request.term >= previousTerm && node !is FollowerNode) {
            node = node.follower()
        }
        rememberLeader(request.term, request.leaderId)
        logLeadershipTransition(previous, node)
        settlePending()
        response
    }

    /** 分割されたInstallSnapshotを受信する。 */
    suspend fun handleInstallSnapshotChunk(chunk: InstallSnapshotChunk): InstallSnapshotChunkResponse = mutex.withLock {
        val previousTerm = node.persistentState.currentTerm
        val response = snapshotHandler.handleChunk(chunk)
        if (chunk.term >= previousTerm && node !is FollowerNode) node = node.follower()
        rememberLeader(chunk.term, chunk.leaderId)
        settlePending()
        response
    }

    /** 適用済み状態を保存してから前方ログを圧縮する。 */
    suspend fun saveSnapshot(metadata: SnapshotMetadata, data: ByteArray) = mutex.withLock {
        saveSnapshotLocked(metadata, data)
    }

    /**
     * 適用済みの MVCC 状態を保存し、その index まで Raft ログを圧縮する。
     * 再起動には [StorageEngineStateMachine.restoreSnapshot]、受信側には
     * [StorageEngineStateMachine.installSnapshot] をスナップショット適用関数として設定する。
     */
    suspend fun saveMvccSnapshot(): SnapshotMetadata = saveAppliedSnapshot {
        storageStateMachine(node).snapshotData()
    }

    internal suspend fun saveAppliedSnapshot(encode: suspend () -> ByteArray): SnapshotMetadata = mutex.withLock {
        val index = node.volatileState.lastApplied
        check(index > 0) { "No applied Raft state to snapshot" }
        val metadata = SnapshotMetadata(index, checkNotNull(node.log.termAt(index)))
        saveSnapshotLocked(metadata, encode())
        metadata
    }

    private suspend fun saveSnapshotLocked(metadata: SnapshotMetadata, data: ByteArray): Unit? {
        check(node.volatileState.lastApplied >= metadata.lastIncludedIndex) { "Snapshot is not applied" }
        require(node.log.termAt(metadata.lastIncludedIndex) == metadata.lastIncludedTerm) { "Snapshot term mismatch" }
        require(metadata.lastIncludedIndex >= (node.log.snapshotMetadata()?.lastIncludedIndex ?: 0L)) {
            "Snapshot must not move backwards"
        }
        val store = checkNotNull(snapshotStore) { "Snapshot store is required for compaction" }
        store.save(metadata, data)
        node.log.compactThrough(metadata, node.volatileState.lastApplied)
        return (node as? LeaderNode)?.updateSnapshotMetadata(metadata)
    }

    suspend fun onElectionTimeout() {
        val election = mutex.withLock {
            if (!node.canVote()) return
            val previous = node
            val result = when (val current = node) {
                is FollowerNode -> current.onElectionTimeout()
                is CandidateNode -> current.onElectionTimeout()
                else -> return
            }
            node = result.node
            settlePending()
            logger.info(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.ELECTION_STARTED,
                mapOf("term" to node.persistentState.currentTerm.toString()),
            )
            logLeadershipTransition(previous, node)
            val candidate = node as? CandidateNode ?: return
            ElectionRound(candidate, result.value, candidate.votingPeers().toList())
        }

        coroutineScope {
            for (peerId in election.peerIds) {
                launch { requestVote(election, peerId) }
            }
        }
    }

    private suspend fun requestVote(election: ElectionRound, peerId: String) {
        peerRpcMutex(peerId).withLock peerLock@{
            if (!mutex.withLock { isCurrentElection(election) }) return@peerLock
            val response = try {
                transport.requestVote(peerId, election.request)
            } catch (_: RaftTransportException) {
                return@peerLock
            }
            mutex.withLock stateLock@{
                if (observeHigherTerm(response.term)) return@stateLock
                if (!isCurrentElection(election)) return@stateLock
                val previous = node
                node = election.candidate.handleRequestVoteResponse(peerId, response)
                logLeadershipTransition(previous, node)
                settlePending()
            }
        }
    }

    suspend fun onHeartbeatTimeout() {
        val round = mutex.withLock {
            val leader = node as? LeaderNode ?: return
            leader.timer.reset(RaftTimeoutEvent.Heartbeat)
            logger.debug(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.HEARTBEAT_SENT,
                mapOf("term" to leader.persistentState.currentTerm.toString()),
            )
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
        logger.debug(
            RaftLogEvents.COMPONENT_RUNTIME,
            RaftLogEvents.COMMAND_APPENDED,
            mapOf(
                "index" to entry.index.toString(),
                "term" to entry.term.toString(),
            ),
        )
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
    suspend fun propose(command: RaftCommand, timeout: Duration = 5.seconds): RaftLogEntry =
        propose(timeout) { it.appendCommand(command) }

    /** リーダーのメモリ MVCC で GC 境界を決め、Raft ログで提案する。 */
    suspend fun proposeCollectGarbage(timeout: Duration = 5.seconds): RaftLogEntry =
        propose(timeout) { leader -> leader.appendGarbageCollection(storageStateMachine(leader).gcSafePoint()) }

    private fun storageStateMachine(node: RaftNode): StorageEngineStateMachine =
        checkNotNull(node.stateMachine as? StorageEngineStateMachine) { "StorageEngineStateMachine required" }

    private suspend fun propose(
        timeout: Duration,
        append: suspend (LeaderNode) -> RaftLogEntry,
    ): RaftLogEntry {
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
                        val entry = append(leader)
                        logger.debug(
                            RaftLogEvents.COMPONENT_RUNTIME,
                            RaftLogEvents.COMMAND_APPENDED,
                            mapOf("index" to entry.index.toString(), "term" to entry.term.toString()),
                        )
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

    internal suspend fun catchUpPeer(peerId: String): Boolean {
        val round = mutex.withLock {
            val leader = node as? LeaderNode ?: throw RaftProposalNotLeaderException()
            leaderRound(leader).also { require(peerId in it.peerIds) { "Unknown peer: $peerId" } }
        }
        return peerRpcMutex(peerId).withLock {
            replicatePeer(round, peerId)
            mutex.withLock {
                isCurrentLeader(round) &&
                    round.leader.peerProgress.getValue(peerId).matchIndex >= round.leader.log.lastIndex()
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
                peerRpcMutex(peerId).withLock {
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
            val needed = mutex.withLock {
                if (!isCurrentLeader(round)) return
                if (snapshotProvider != null || snapshotStore != null) round.leader.snapshotFor(peerId) else null
            }
            val metadata = needed
            if (metadata != null) {
                if (!replicateSnapshot(round, peerId, metadata)) return
            } else {
                if (!replicateAppend(round, peerId)) return
            }
        }
    }

    /**
     * スナップショットを送信し、終端確認後に複製位置を進める。
     * 送信は mutex を保持せずに行い、[snapshotProvider] の取得失敗時は
     * 複製を打ち切る（呼び出し側の供給責務）。
     */
    private suspend fun replicateSnapshot(
        round: LeaderRound,
        peerId: String,
        metadata: SnapshotMetadata,
    ): Boolean {
        val data = try {
            snapshotProvider?.snapshotData(metadata) ?: snapshotStore?.load()?.let {
                check(it.first == metadata) { "Snapshot generation changed" }
                it.second
            } ?: return false
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            return false
        }
        val attempt = mutex.withLock {
            if (!isCurrentLeader(round)) return false
            val request = round.leader.createInstallSnapshot(peerId, data)
            if (request.metadata != metadata) return false
            SnapshotAttempt(round, request).also {
                snapshotAttempts[peerId] = it
            }
        }
        logger.debug(
            RaftLogEvents.COMPONENT_RUNTIME,
            RaftLogEvents.INSTALL_SNAPSHOT_SENT,
            mapOf(
                "peerId" to peerId,
                "term" to attempt.request.term.toString(),
            ),
        )
        val response = try {
            val limit = snapshotChunkBytes
            if (limit == null) {
                transport.installSnapshot(peerId, attempt.request)
            } else {
                for (chunk in InstallSnapshotChunks.chunked(attempt.request.term, attempt.request.leaderId,
                    metadata, data, limit)) {
                    if (!mutex.withLock { isCurrentLeader(round) }) return false
                    val result = transport.installSnapshotChunk(peerId, chunk)
                    if (mutex.withLock { observeHigherTerm(result.term) }) return false
                    if (result.term != attempt.request.term || !result.accepted ||
                        result.nextOffset != chunk.offset + chunk.data.size) return false
                }
                InstallSnapshotResponse(attempt.request.term)
            }
        } catch (_: RaftTransportException) {
            return false
        }
        return mutex.withLock {
            if (observeHigherTerm(response.term)) return@withLock false
            if (
                !isCurrentLeader(attempt.round) ||
                snapshotAttempts[peerId] !== attempt ||
                response.term != attempt.request.term
            ) {
                return@withLock false
            }
            snapshotAttempts.remove(peerId)
            node = round.leader.handleInstallSnapshotResponse(peerId, attempt.request, response)
            logReplicationResult(peerId, true)
            settlePending()
            isCurrentLeader(round)
        }
    }

    /** AppendEntries を1往復送る。継続する場合に真を返す。 */
    private suspend fun replicateAppend(
        round: LeaderRound,
        peerId: String,
    ): Boolean {
        val attempt = mutex.withLock {
            if (!isCurrentLeader(round)) return false
            AppendAttempt(round, round.leader.createAppendEntries(peerId)).also {
                appendAttempts[peerId] = it
            }
        }
        val response = try {
            transport.appendEntries(peerId, attempt.request)
        } catch (_: RaftTransportException) {
            return false
        }
        return mutex.withLock {
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
            logReplicationResult(peerId, response.success)
            settlePending()
            !response.success && isCurrentLeader(round)
        }
    }

    private suspend fun rememberLeader(term: Long, peerId: String) {
        if (node is FollowerNode && term == node.persistentState.currentTerm && peerId in node.votingPeers()) {
            leaderHint = LeaderHint(term, peerId)
        }
    }

    private data class LeaderHint(val term: Long, val peerId: String)

    private fun isCurrentElection(round: ElectionRound): Boolean =
        node === round.candidate && node.persistentState.currentTerm == round.request.term

    private suspend fun leaderRound(leader: LeaderNode) = LeaderRound(
        leader,
        leader.persistentState.currentTerm,
        leader.refreshReplicationPeers().toList(),
    )

    private fun isCurrentLeader(round: LeaderRound): Boolean =
        node === round.leader && node.persistentState.currentTerm == round.term

    // Higher terms remain authoritative even when the original RPC is stale.
    private suspend fun observeHigherTerm(term: Long): Boolean {
        if (term <= node.persistentState.currentTerm) return false
        node.persistentStateStore.save(term, null)
        node.persistentState.currentTerm = term
        node.persistentState.votedFor = null
        val previous = node
        node = node.follower()
        logLeadershipTransition(previous, node)
        appendAttempts.clear()
        snapshotAttempts.clear()
        settlePending()
        node.timer.reset(RaftTimeoutEvent.Election)
        return true
    }

    /** リーダー昇格/降格を検出して記録する。 */
    private fun logLeadershipTransition(from: RaftNode, to: RaftNode) {
        if (from is LeaderNode && to !is LeaderNode) {
            logger.warn(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.LEADER_DEMOTED,
                mapOf("term" to to.persistentState.currentTerm.toString()),
            )
        } else if (from !is LeaderNode && to is LeaderNode) {
            logger.info(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.LEADER_ELECTED,
                mapOf("term" to to.persistentState.currentTerm.toString()),
            )
        }
    }

    /** レプリケーション成否を記録する。 */
    private fun logReplicationResult(peerId: String, success: Boolean) {
        if (success) {
            logger.debug(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.REPLICATION_SUCCEEDED,
                mapOf("peerId" to peerId),
            )
        } else {
            logger.warn(
                RaftLogEvents.COMPONENT_RUNTIME,
                RaftLogEvents.REPLICATION_FAILED,
                mapOf("peerId" to peerId),
            )
        }
    }

    private suspend fun settlePending() {
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

    private suspend fun settleReads() {
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
                read.configuration?.let { it != read.round.leader.votingConfiguration() } == true -> {
                    read.completion.completeExceptionally(RaftReadLeadershipLostException(read.round.term))
                    iterator.remove()
                }
                else -> {
                    if (read.index == null && read.round.leader.isReady) {
                        // Snapshot before sending probes; older RPCs cannot satisfy this context.
                        read.index = node.volatileState.commitIndex
                        val configuration = read.round.leader.votingConfiguration()
                        read.configuration = configuration
                        val scope = CoroutineScope(proposalScope.coroutineContext + read.probeJob)
                        for (peerId in configuration.voters - node.nodeId) {
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
                        checkNotNull(read.configuration).hasQuorum(read.confirmedPeers) &&
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
        peerRpcMutex(peerId).withLock peerLock@{
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
        var configuration: RaftVotingConfiguration? = null
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

    private class SnapshotAttempt(
        val round: LeaderRound,
        val request: InstallSnapshotRequest,
    )
}
