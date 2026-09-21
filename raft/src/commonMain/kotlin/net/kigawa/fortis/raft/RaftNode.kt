package net.kigawa.fortis.raft

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.append.*
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteHandler
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RaftNode(
    val nodeId: String,
    private val peers: MutableMap<String, RaftPeerProgress>,
    private val persistentState: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val stateMachine: RaftStateMachine,
) {
    private val mutex = Mutex()
    private val votesGranted =
        mutableSetOf<String>()

    var role: RaftRole = RaftRole.FOLLOWER
        private set
    private val requestVoteHandler =
        RequestVoteHandler(
            state = persistentState,
            log = log,
        )

    private val appendEntriesHandler =
        AppendEntriesHandler(
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        )

    private val commitAdvancer =
        RaftCommitAdvancer(
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
        )

    private val appendEntriesResponseHandler =
        AppendEntriesResponseHandler(
            persistentState = persistentState,
            commitAdvancer = commitAdvancer,
        )

    private val appendEntriesFactory =
        AppendEntriesFactory(
            nodeId = nodeId,
            state = persistentState,
            volatileState = volatileState,
            log = log,
        )

    suspend fun handleRequestVote(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        val previousTerm =
            persistentState.currentTerm

        val response =
            requestVoteHandler.handle(request)

        if (
            persistentState.currentTerm >
            previousTerm
        ) {
            role = RaftRole.FOLLOWER
        }

        response
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        val response =
            appendEntriesHandler.handle(request)

        if (request.term >= persistentState.currentTerm) {
            role = RaftRole.FOLLOWER
        }

        response
    }

    suspend fun createAppendEntries(
        peerId: String,
    ): AppendEntriesRequest = mutex.withLock {
        check(role == RaftRole.LEADER) {
            "Only leader can create AppendEntries"
        }

        val progress =
            requireNotNull(peers[peerId]) {
                "Unknown peer: $peerId"
            }

        appendEntriesFactory.create(
            progress,
        )
    }

    suspend fun handleAppendEntriesResponse(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ): Unit = mutex.withLock {
        if (role != RaftRole.LEADER) {
            return@withLock
        }
        val progress =
            requireNotNull(peers[peerId]) {
                "Unknown peer: $peerId"
            }

        val previousTerm =
            persistentState.currentTerm

        appendEntriesResponseHandler.handle(
            progress = progress,
            peers = peers.values,
            request = request,
            response = response,
        )

        if (
            persistentState.currentTerm >
            previousTerm
        ) {
            role = RaftRole.FOLLOWER
        }
    }

    suspend fun startElection(): RequestVoteRequest =
        mutex.withLock {
            check(persistentState.currentTerm < Long.MAX_VALUE) {
                "Raft term is exhausted"
            }

            persistentState.currentTerm++
            persistentState.votedFor = nodeId
            role = RaftRole.CANDIDATE
            votesGranted.clear()
            votesGranted.add(nodeId)

            if (hasMajority()) {
                becomeLeader()
            }

            val lastLogIndex = log.lastIndex()
            RequestVoteRequest(
                term = persistentState.currentTerm,
                candidateId = nodeId,
                lastLogIndex = lastLogIndex,
                lastLogTerm = log.get(lastLogIndex)?.term ?: 0L,
            )
        }

    suspend fun handleRequestVoteResponse(
        peerId: String,
        response: RequestVoteResponse,
    ): Unit = mutex.withLock {
        require(peers.containsKey(peerId)) {
            "Unknown peer: $peerId"
        }

        if (response.term > persistentState.currentTerm) {
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            votesGranted.clear()
            role = RaftRole.FOLLOWER
            return@withLock
        }

        if (
            role != RaftRole.CANDIDATE ||
            response.term != persistentState.currentTerm ||
            !response.voteGranted
        ) {
            return@withLock
        }

        votesGranted.add(peerId)
        if (hasMajority()) {
            becomeLeader()
        }
    }

    private fun hasMajority(): Boolean {
        val clusterSize = peers.size + 1
        return votesGranted.size >= clusterSize / 2 + 1
    }

    private suspend fun becomeLeader() {
        role = RaftRole.LEADER
        val nextIndex = log.lastIndex() + 1
        for (progress in peers.values) {
            progress.nextIndex = nextIndex
            progress.matchIndex = 0
        }
    }
}
