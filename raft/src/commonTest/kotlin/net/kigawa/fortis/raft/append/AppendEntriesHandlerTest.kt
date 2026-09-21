package net.kigawa.fortis.raft.append

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AppendEntriesHandlerTest {
    @Test
    fun olderTermIsRejected() = runTest {
        val fixture = fixture(currentTerm = 2)

        val response = fixture.handler.handle(request(term = 1))

        assertFalse(response.success)
        assertEquals(2L, response.term)
        assertEquals(2L, fixture.persistentState.currentTerm)
        assertEquals(0L, fixture.log.lastIndex())
    }

    @Test
    fun newerTermUpdatesCurrentTermAndResetsVote() = runTest {
        val fixture = fixture(
            currentTerm = 1,
            votedFor = "candidate",
        )

        val response = fixture.handler.handle(request(term = 2))

        assertTrue(response.success)
        assertEquals(2L, response.term)
        assertEquals(2L, fixture.persistentState.currentTerm)
        assertNull(fixture.persistentState.votedFor)
    }

    @Test
    fun zeroPreviousLogIndexIsAccepted() = runTest {
        val fixture = fixture(currentTerm = 1)

        val response = fixture.handler.handle(
            request(
                term = 1,
                prevLogIndex = 0,
                prevLogTerm = 99,
            )
        )

        assertTrue(response.success)
    }

    @Test
    fun missingPreviousEntryIsRejected() = runTest {
        val fixture = fixture(currentTerm = 1)

        val response = fixture.handler.handle(
            request(
                term = 1,
                prevLogIndex = 1,
                prevLogTerm = 1,
            )
        )

        assertFalse(response.success)
    }

    @Test
    fun mismatchedPreviousTermIsRejected() = runTest {
        val fixture = fixture(currentTerm = 2)
        fixture.log.append(entry(index = 1, term = 1))

        val response = fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 1,
                prevLogTerm = 2,
            )
        )

        assertFalse(response.success)
        assertEquals(entry(index = 1, term = 1), fixture.log.get(1))
    }

    @Test
    fun appendsEntryAfterMatchingPreviousEntry() = runTest {
        val fixture = fixture(currentTerm = 2)
        fixture.log.append(entry(index = 1, term = 1))
        val appended = entry(index = 2, term = 2)

        val response = fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 1,
                prevLogTerm = 1,
                entries = listOf(appended),
            )
        )

        assertTrue(response.success)
        assertEquals(appended, fixture.log.get(2))
        assertEquals(2L, fixture.log.lastIndex())
    }

    @Test
    fun conflictingTermTruncatesAndReplacesExistingEntries() = runTest {
        val fixture = fixture(currentTerm = 2)
        fixture.log.append(entry(index = 1, term = 1))
        fixture.log.append(entry(index = 2, term = 1))
        fixture.log.append(entry(index = 3, term = 1))
        val replacement = entry(index = 2, term = 2)

        val response = fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 1,
                prevLogTerm = 1,
                entries = listOf(replacement),
            )
        )

        assertTrue(response.success)
        assertEquals(replacement, fixture.log.get(2))
        assertNull(fixture.log.get(3))
        assertEquals(2L, fixture.log.lastIndex())
    }

    @Test
    fun heartbeatWithNoEntriesSucceedsWithoutChangingLog() = runTest {
        val fixture = fixture(currentTerm = 2)
        val existing = entry(index = 1, term = 1)
        fixture.log.append(existing)

        val response = fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 1,
                prevLogTerm = 1,
                entries = emptyList(),
            )
        )

        assertTrue(response.success)
        assertEquals(existing, fixture.log.get(1))
        assertEquals(1L, fixture.log.lastIndex())
    }

    @Test
    fun leaderCommitAdvancesCommitIndex() = runTest {
        val fixture = fixtureWithEntries(2)

        val response = fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 2,
                prevLogTerm = 1,
                leaderCommit = 1,
            )
        )

        assertTrue(response.success)
        assertEquals(1L, fixture.volatileState.commitIndex)
    }

    @Test
    fun committedEntriesAreAppliedToStateMachine() = runTest {
        val fixture = fixtureWithEntries(2)

        fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 2,
                prevLogTerm = 1,
                leaderCommit = 2,
            )
        )

        assertEquals(
            listOf<RaftCommand>(command(1), command(2)),
            fixture.stateMachine.applied,
        )
        assertEquals(2L, fixture.volatileState.lastApplied)
    }

    @Test
    fun leaderCommitBeyondLastIndexCommitsOnlyThroughLastEntry() = runTest {
        val fixture = fixtureWithEntries(2)

        fixture.handler.handle(
            request(
                term = 2,
                prevLogIndex = 2,
                prevLogTerm = 1,
                leaderCommit = 10,
            )
        )

        assertEquals(2L, fixture.volatileState.commitIndex)
        assertEquals(2L, fixture.volatileState.lastApplied)
        assertEquals(
            listOf<RaftCommand>(command(1), command(2)),
            fixture.stateMachine.applied,
        )
    }

    private suspend fun fixtureWithEntries(count: Int): Fixture =
        fixture(currentTerm = 2).also { fixture ->
            for (index in 1L..count.toLong()) {
                fixture.log.append(entry(index = index, term = 1))
            }
        }

    private fun fixture(
        currentTerm: Long,
        votedFor: String? = null,
    ): Fixture {
        val persistentState = RaftPersistentState(currentTerm, votedFor)
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        return Fixture(
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
            handler = AppendEntriesHandler(
                persistentState = persistentState,
                volatileState = volatileState,
                log = log,
                stateMachine = stateMachine,
            ),
        )
    }

    private fun request(
        term: Long,
        prevLogIndex: Long = 0,
        prevLogTerm: Long = 0,
        entries: List<RaftLogEntry> = emptyList(),
        leaderCommit: Long = 0,
    ) = AppendEntriesRequest(
        term = term,
        leaderId = "leader",
        prevLogIndex = prevLogIndex,
        prevLogTerm = prevLogTerm,
        entries = entries,
        leaderCommit = leaderCommit,
    )

    private fun entry(
        index: Long,
        term: Long,
    ) = RaftLogEntry(
        index = index,
        term = term,
        command = command(index),
    )

    private fun command(index: Long) =
        RaftCommand.Put(
            key = byteArrayOf(index.toByte()),
            value = byteArrayOf((index * 10).toByte()),
        )

    private data class Fixture(
        val persistentState: RaftPersistentState,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
        val handler: AppendEntriesHandler,
    )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
