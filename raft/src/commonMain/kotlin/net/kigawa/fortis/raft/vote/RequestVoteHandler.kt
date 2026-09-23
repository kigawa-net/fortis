package net.kigawa.fortis.raft.vote

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.RaftLog


class RequestVoteHandler(
    private val state: RaftPersistentState,
    private val stateStore: RaftPersistentStateStore,
    private val log: RaftLog,
) {
    private val mutex = Mutex()
    suspend fun handle(
        request: RequestVoteRequest,
    ): RequestVoteResponse = handleLock(request)

    internal suspend fun handleLock(
        request: RequestVoteRequest,
    ): RequestVoteResponse = mutex.withLock {
        if (request.term < state.currentTerm) {
            return RequestVoteResponse(
                term = state.currentTerm,
                voteGranted = false,
            )
        }

        val nextTerm = maxOf(state.currentTerm, request.term)
        val previousVote = if (request.term > state.currentTerm) {
            null
        } else {
            state.votedFor
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

        val canVote = previousVote == null ||
            previousVote == request.candidateId

        val voteGranted =
            canVote && logUpToDate

        val nextVote = if (voteGranted) {
            request.candidateId
        } else {
            previousVote
        }
        if (nextTerm != state.currentTerm || nextVote != state.votedFor) {
            stateStore.save(nextTerm, nextVote)
            state.currentTerm = nextTerm
            state.votedFor = nextVote
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
