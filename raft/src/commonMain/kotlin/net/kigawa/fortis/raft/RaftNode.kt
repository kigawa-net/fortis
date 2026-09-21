package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.append.*
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteHandler
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RaftNode(
    val nodeId: String,
    private val peers: Map<String, RaftPeerProgress>,
    private val persistentState: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val stateMachine: RaftStateMachine,
) {
    private val votesGranted =
        mutableSetOf<String>()
    private val peerProgress =
        mutableMapOf<String, RaftPeerProgress>()
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
    ): RequestVoteResponse {
        TODO()
    }

    suspend fun handleAppendEntries(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse {
        val response =
            appendEntriesHandler.handle(request)

        if (request.term >= persistentState.currentTerm) {
            role = RaftRole.FOLLOWER
        }

        return response
    }

    suspend fun createAppendEntries(
        peerId: String,
    ): AppendEntriesRequest {
        TODO()
    }

    suspend fun handleAppendEntriesResponse(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ) {
        TODO()
    }
}