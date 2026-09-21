package net.kigawa.fortis.raft.append

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState

class AppendEntriesHandler(
    private val persistentState: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val stateMachine: RaftStateMachine,
) {
    private val mutex = Mutex()

    suspend fun handle(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = mutex.withLock {
        if (request.term < persistentState.currentTerm) {
            return@withLock response(success = false)
        }

        if (request.term > persistentState.currentTerm) {
            persistentState.currentTerm = request.term
            persistentState.votedFor = null
        }

        if (!previousEntryMatches(request)) {
            return@withLock response(success = false)
        }

        appendEntries(request.entries)
        advanceCommitIndex(request.leaderCommit)

        response(success = true)
    }

    private suspend fun previousEntryMatches(
        request: AppendEntriesRequest,
    ): Boolean {
        if (request.prevLogIndex == 0L) {
            return true
        }

        val previousEntry =
            log.get(request.prevLogIndex) ?: return false
        return previousEntry.term == request.prevLogTerm
    }

    private suspend fun appendEntries(
        entries: List<RaftLogEntry>,
    ) {
        for (entry in entries) {
            val existing = log.get(entry.index)
            if (existing?.term == entry.term) {
                continue
            }
            if (existing != null) {
                log.truncateFrom(entry.index)
            }
            log.append(entry)
        }
    }

    private suspend fun advanceCommitIndex(leaderCommit: Long) {
        if (leaderCommit > volatileState.commitIndex) {
            volatileState.commitIndex =
                minOf(leaderCommit, log.lastIndex())
        }

        while (volatileState.lastApplied < volatileState.commitIndex) {
            val nextIndex = volatileState.lastApplied + 1
            val entry = checkNotNull(log.get(nextIndex)) {
                "Committed Raft log entry is missing at index $nextIndex"
            }
            stateMachine.apply(entry.command)
            volatileState.lastApplied = nextIndex
        }
    }

    private fun response(success: Boolean) =
        AppendEntriesResponse(
            term = persistentState.currentTerm,
            success = success,
        )
}
