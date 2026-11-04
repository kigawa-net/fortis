package net.kigawa.fortis.raft.append

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class AppendEntriesResponseHandlerTest {
    @Test
    fun successfulResponseUpdatesMatchAndNextIndexes() = runTest {
        val fixture = fixture(currentTerm = 2)
        val entries = listOf(
            entry(index = 1, term = 2),
            entry(index = 2, term = 2),
        )
        for (entry in entries) {
            fixture.log.append(entry)
        }
        val progress = RaftPeerProgress(nextIndex = 1)

        val updatedPeers = fixture.handler.handle(
            peerId = "peer",
            peers = mapOf(
                "peer" to progress,
                "other" to RaftPeerProgress(nextIndex = 1),
            ),
            request = request(term = 2, entries = entries),
            response = AppendEntriesResponse(term = 2, success = true),
        )

        assertEquals(2L, updatedPeers.getValue("peer").matchIndex)
        assertEquals(3L, updatedPeers.getValue("peer").nextIndex)
    }

    @Test
    fun failedResponseDecrementsNextIndex() = runTest {
        val fixture = fixture(currentTerm = 2)
        val progress = RaftPeerProgress(nextIndex = 5)

        val updatedPeers = fixture.handler.handle(
            peerId = "peer",
            peers = mapOf("peer" to progress),
            request = request(term = 2),
            response = AppendEntriesResponse(term = 2, success = false),
        )

        assertEquals(4L, updatedPeers.getValue("peer").nextIndex)
        assertEquals(0L, updatedPeers.getValue("peer").matchIndex)
    }

    @Test
    fun failedResponseDoesNotDecrementNextIndexBelowOne() = runTest {
        val fixture = fixture(currentTerm = 2)
        val progress = RaftPeerProgress(nextIndex = 1)

        val updatedPeers = fixture.handler.handle(
            peerId = "peer",
            peers = mapOf("peer" to progress),
            request = request(term = 2),
            response = AppendEntriesResponse(term = 2, success = false),
        )

        assertEquals(1L, updatedPeers.getValue("peer").nextIndex)
    }

    @Test
    fun higherResponseTermUpdatesTermAndResetsVote() = runTest {
        val fixture = fixture(
            currentTerm = 2,
            votedFor = "self",
        )
        val progress = RaftPeerProgress(nextIndex = 3, matchIndex = 2)

        fixture.handler.handle(
            peerId = "peer",
            peers = mapOf("peer" to progress),
            request = request(term = 2),
            response = AppendEntriesResponse(term = 3, success = false),
        )

        assertEquals(3L, fixture.persistentState.currentTerm)
        assertNull(fixture.persistentState.votedFor)
        assertEquals(
            RaftPersistentState(3, null),
            fixture.persistentStateStore.load(),
        )
        assertEquals(3L, progress.nextIndex)
        assertEquals(2L, progress.matchIndex)
    }

    private fun fixture(
        currentTerm: Long,
        votedFor: String? = null,
    ): Fixture {
        val persistentState = RaftPersistentState(currentTerm, votedFor)
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val persistentStateStore = MemoryRaftPersistentStateStore(
            persistentState,
        )
        return Fixture(
            persistentState = persistentState,
            persistentStateStore = persistentStateStore,
            log = log,
            handler = AppendEntriesResponseHandler(
                persistentState = persistentState,
                persistentStateStore = persistentStateStore,
                commitAdvancer = RaftCommitAdvancer(
                    persistentState = persistentState,
                    volatileState = volatileState,
                    log = log,
                ),
                applier = RaftApplier(
                    volatileState = volatileState,
                    log = log,
                    stateMachine = NoOpStateMachine(),
                ),
            ),
        )
    }

    private fun request(
        term: Long,
        entries: List<RaftLogEntry> = emptyList(),
    ) = AppendEntriesRequest(
        term = term,
        leaderId = "leader",
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = entries,
        leaderCommit = 0,
    )

    private fun entry(index: Long, term: Long) =
        RaftLogEntry(
            index = index,
            term = term,
            command = RaftCommand.Delete(byteArrayOf(index.toByte())),
        )

    private data class Fixture(
        val persistentState: RaftPersistentState,
        val persistentStateStore: MemoryRaftPersistentStateStore,
        val log: MemoryRaftLog,
        val handler: AppendEntriesResponseHandler,
    )

    private class NoOpStateMachine : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }
}
