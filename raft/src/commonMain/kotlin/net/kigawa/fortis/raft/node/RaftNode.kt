package net.kigawa.fortis.raft.node

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesHandler
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState
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
        val response = requestVoteHandler.handle(request)
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
