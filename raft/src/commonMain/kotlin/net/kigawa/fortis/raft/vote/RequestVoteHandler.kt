package net.kigawa.fortis.raft.vote

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.log.RaftLog


data class RequestVoteHandler(
    val state: RaftPersistentState,
    val log: RaftLog,
    val persistentState: RaftPersistentState,
) {
    private val mutex = Mutex()
    suspend fun handle(
        request: RequestVoteRequest,
        setRole: (RaftRole) -> Unit,
    ): RequestVoteResponse {
        val previousTerm =
            persistentState.currentTerm

        val response = handleLock(request)

        if (
            persistentState.currentTerm >
            previousTerm
        ) {
            setRole(RaftRole.FOLLOWER)
        }

        return response
    }

    internal suspend fun handleLock(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        if (request.term < state.currentTerm) {
            return RequestVoteResponse(
                term = state.currentTerm,
                voteGranted = false,
            )
        }

        if (request.term > state.currentTerm) {
            state.currentTerm = request.term
            state.votedFor = null
        }

        val lastIndex = log.lastIndex()
        val lastTerm =
            log.get(lastIndex)?.term ?: 0L

        val logUpToDate =
            isCandidateLogUpToDate(
                candidateLastTerm = request.lastLogTerm,
                candidateLastIndex = request.lastLogIndex,
                localLastTerm = lastTerm,
                localLastIndex = lastIndex,
            )

        val canVote =
            state.votedFor == null ||
                state.votedFor == request.candidateId

        val voteGranted =
            canVote && logUpToDate

        if (voteGranted) {
            state.votedFor = request.candidateId
        }

        return RequestVoteResponse(
            term = state.currentTerm,
            voteGranted = voteGranted,
        )
    }

    private fun isCandidateLogUpToDate(
        candidateLastTerm: Long,
        candidateLastIndex: Long,
        localLastTerm: Long,
        localLastIndex: Long,
    ): Boolean =
        when {
            candidateLastTerm > localLastTerm -> true
            candidateLastTerm < localLastTerm -> false
            else -> candidateLastIndex >= localLastIndex
        }
}
