package net.kigawa.fortis.raft.vote

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RequestVoteHandlerTest {
    @Test
    fun olderTermIsRejected() = runTest {
        val state = RaftPersistentState(currentTerm = 2)
        val response = handler(state).handleLock(request(term = 1))

        assertFalse(response.voteGranted)
        assertEquals(2L, response.term)
        assertEquals(2L, state.currentTerm)
        assertNull(state.votedFor)
    }

    @Test
    fun newerTermUpdatesCurrentTermAndResetsVote() = runTest {
        val state = RaftPersistentState(
            currentTerm = 1,
            votedFor = "previous-candidate",
        )
        val log = MemoryRaftLog().also {
            it.append(entry(index = 1, term = 2))
        }
        val store = MemoryRaftPersistentStateStore(state)

        val response = RequestVoteHandler(state, store, log).handleLock(
            request(
                term = 2,
                candidateId = "new-candidate",
                lastLogIndex = 1,
                lastLogTerm = 1,
            )
        )

        assertEquals(2L, state.currentTerm)
        assertEquals(2L, response.term)
        assertFalse(response.voteGranted)
        assertNull(state.votedFor)
        assertEquals(RaftPersistentState(2, null), store.load())
    }

    @Test
    fun voteIsNotGrantedWhenPersistenceFails() = runTest {
        val state = RaftPersistentState(currentTerm = 1)
        val handler = RequestVoteHandler(
            state,
            FailingStateStore(state),
            MemoryRaftLog(),
        )

        assertFailsWith<IllegalStateException> {
            handler.handle(request(term = 1))
        }

        assertNull(state.votedFor)
    }

    @Test
    fun grantsVoteWhenNotYetVoted() = runTest {
        val state = RaftPersistentState(currentTerm = 1)

        val response = handler(state).handleLock(request(term = 1))

        assertTrue(response.voteGranted)
        assertEquals("candidate", state.votedFor)
    }

    @Test
    fun grantsVoteAgainToSameCandidate() = runTest {
        val state = RaftPersistentState(
            currentTerm = 1,
            votedFor = "candidate",
        )

        val response = handler(state).handleLock(request(term = 1))

        assertTrue(response.voteGranted)
        assertEquals("candidate", state.votedFor)
    }

    @Test
    fun rejectsDifferentCandidateInSameTerm() = runTest {
        val state = RaftPersistentState(
            currentTerm = 1,
            votedFor = "first-candidate",
        )

        val response = handler(state).handleLock(
            request(term = 1, candidateId = "second-candidate")
        )

        assertFalse(response.voteGranted)
        assertEquals("first-candidate", state.votedFor)
    }

    @Test
    fun rejectsCandidateWithOlderLog() = runTest {
        val state = RaftPersistentState(currentTerm = 3)
        val log = MemoryRaftLog().also {
            it.append(entry(index = 1, term = 2))
        }

        val response = handler(state, log).handleLock(
            request(
                term = 3,
                lastLogIndex = 10,
                lastLogTerm = 1,
            )
        )

        assertFalse(response.voteGranted)
        assertNull(state.votedFor)
    }

    @Test
    fun grantsCandidateWithNewerLog() = runTest {
        val state = RaftPersistentState(currentTerm = 3)
        val log = MemoryRaftLog().also {
            it.append(entry(index = 1, term = 1))
            it.append(entry(index = 2, term = 1))
        }

        val response = handler(state, log).handleLock(
            request(
                term = 3,
                lastLogIndex = 1,
                lastLogTerm = 2,
            )
        )

        assertTrue(response.voteGranted)
        assertEquals("candidate", state.votedFor)
    }

    @Test
    fun rejectsShorterCandidateLogWhenLastTermsAreEqual() = runTest {
        val state = RaftPersistentState(currentTerm = 3)
        val log = MemoryRaftLog().also {
            it.append(entry(index = 1, term = 1))
            it.append(entry(index = 2, term = 2))
        }

        val response = handler(state, log).handleLock(
            request(
                term = 3,
                lastLogIndex = 1,
                lastLogTerm = 2,
            )
        )

        assertFalse(response.voteGranted)
        assertNull(state.votedFor)
    }

    @Test
    fun grantsCandidateWhenLastTermAndIndexMatchLocalLog() = runTest {
        val state = RaftPersistentState(currentTerm = 3)
        val log = MemoryRaftLog().also {
            it.append(entry(index = 1, term = 1))
            it.append(entry(index = 2, term = 2))
        }

        val response = handler(state, log).handleLock(
            request(
                term = 3,
                lastLogIndex = 2,
                lastLogTerm = 2,
            )
        )

        assertTrue(response.voteGranted)
        assertEquals("candidate", state.votedFor)
    }

    private fun handler(
        state: RaftPersistentState,
        log: MemoryRaftLog = MemoryRaftLog(),
    ) = RequestVoteHandler(
        state,
        MemoryRaftPersistentStateStore(state),
        log,
    )

    private class FailingStateStore(
        private val state: RaftPersistentState,
    ) : RaftPersistentStateStore {
        override suspend fun load(): RaftPersistentState = state.copy()

        override suspend fun save(term: Long, votedFor: String?): Nothing {
            error("save failed")
        }
    }

    private fun request(
        term: Long,
        candidateId: String = "candidate",
        lastLogIndex: Long = 0,
        lastLogTerm: Long = 0,
    ) = RequestVoteRequest(
        term = term,
        candidateId = candidateId,
        lastLogIndex = lastLogIndex,
        lastLogTerm = lastLogTerm,
    )

    private fun entry(
        index: Long,
        term: Long,
    ) = RaftLogEntry(
        index = index,
        term = term,
        command = RaftCommand.Delete(byteArrayOf(index.toByte())),
    )
}
