package net.kigawa.fortis.raft.node

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftCommitQuorum
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesHandler
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.meta.MetaRaftStateMachine
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RaftVotingConfiguration
import net.kigawa.fortis.raft.vote.RequestVoteHandler
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

abstract class RaftNode(
    val nodeId: String,
    val peerIds: Set<String>,
    val persistentState: RaftPersistentState,
    val persistentStateStore: RaftPersistentStateStore,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
    val stateMachine: RaftStateMachine,
    val timer: RaftTimer,
) {
    internal val metaStateMachine = stateMachine as? MetaRaftStateMachine

    internal suspend fun votingConfiguration(): RaftVotingConfiguration =
        metaStateMachine?.votingConfiguration(log, volatileState.lastApplied) ?: RaftVotingConfiguration(peerIds + nodeId)

    internal suspend fun votingPeers(): Set<String> = votingConfiguration().voters - nodeId

    internal suspend fun canVote(): Boolean = metaStateMachine == null || nodeId in votingConfiguration().voters

    internal suspend fun replicationPeers(): Set<String> =
        metaStateMachine?.let { (it.members() + votingPeers()) - nodeId } ?: peerIds

    internal suspend fun quorumProgress(peers: Map<String, RaftPeerProgress>): List<RaftCommitQuorum> =
        if (metaStateMachine == null) {
            listOf(RaftCommitQuorum(peers.values))
        } else {
            votingConfiguration().quorums.map { quorum ->
                RaftCommitQuorum(
                    (quorum - nodeId).map { peers[it] ?: RaftPeerProgress(nextIndex = 1) },
                    includesLeader = nodeId in quorum,
                )
            }
        }

    private val mutex = Mutex()
    private val applier = RaftApplier(volatileState, log, stateMachine)
    private val requestVoteHandler = RequestVoteHandler(
        persistentState,
        persistentStateStore,
        log,
    )
    private val appendEntriesHandler = AppendEntriesHandler(
        persistentState,
        persistentStateStore,
        volatileState,
        log,
        applier,
    )

    internal val commitAdvancer = RaftCommitAdvancer(
        persistentState,
        volatileState,
        log,
    )
    internal val raftApplier: RaftApplier
        get() = applier

    suspend fun handleRequestVote(
        request: RequestVoteRequest,
    ): RaftNodeResult<RequestVoteResponse> = mutex.withLock {
        val previousTerm = persistentState.currentTerm
        val response = if (canVote()) {
            requestVoteHandler.handle(request)
        } else {
            if (request.term > previousTerm) {
                persistentStateStore.save(request.term, null)
                persistentState.currentTerm = request.term
                persistentState.votedFor = null
            }
            RequestVoteResponse(persistentState.currentTerm, false)
        }
        if (response.voteGranted) {
            timer.reset(RaftTimeoutEvent.Election)
        }
        val nextNode = if (
            request.term > previousTerm && this !is FollowerNode
        ) {
            follower()
        } else {
            this
        }
        RaftNodeResult(nextNode, response)
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): RaftNodeResult<AppendEntriesResponse> = mutex.withLock {
        val previousTerm = persistentState.currentTerm
        val response = appendEntriesHandler.handle(request)
        val validLeaderTerm = request.term >= previousTerm
        if (validLeaderTerm) {
            timer.reset(RaftTimeoutEvent.Election)
        }
        val nextNode = if (validLeaderTerm && this !is FollowerNode) {
            follower()
        } else {
            this
        }
        RaftNodeResult(nextNode, response)
    }

    internal fun follower(): FollowerNode = FollowerNode(
        nodeId,
        peerIds,
        persistentState,
        persistentStateStore,
        volatileState,
        log,
        stateMachine,
        timer,
    )
}
