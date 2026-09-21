package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals

class RaftCommitAdvancerTest {
    @Test
    fun twoReplicasInThreeNodeClusterCommitEntry() = runTest {
        val fixture = fixture(currentTerm = 2, entryTerms = listOf(2))
        val peers = listOf(
            RaftPeerProgress(nextIndex = 2, matchIndex = 1),
            RaftPeerProgress(nextIndex = 1, matchIndex = 0),
        )

        fixture.advancer.advance(peers)

        assertEquals(1L, fixture.volatileState.commitIndex)
    }

    @Test
    fun oneReplicaInThreeNodeClusterDoesNotCommitEntry() = runTest {
        val fixture = fixture(currentTerm = 2, entryTerms = listOf(2))
        val peers = listOf(
            RaftPeerProgress(nextIndex = 1, matchIndex = 0),
            RaftPeerProgress(nextIndex = 1, matchIndex = 0),
        )

        fixture.advancer.advance(peers)

        assertEquals(0L, fixture.volatileState.commitIndex)
    }

    @Test
    fun oldTermEntryIsNotCommittedEvenWithMajority() = runTest {
        val fixture = fixture(currentTerm = 2, entryTerms = listOf(1))
        val peers = listOf(
            RaftPeerProgress(nextIndex = 2, matchIndex = 1),
            RaftPeerProgress(nextIndex = 2, matchIndex = 1),
        )

        fixture.advancer.advance(peers)

        assertEquals(0L, fixture.volatileState.commitIndex)
    }

    @Test
    fun currentTermEntryIsCommittedWithMajority() = runTest {
        val fixture = fixture(currentTerm = 2, entryTerms = listOf(1, 2))
        val peers = listOf(
            RaftPeerProgress(nextIndex = 3, matchIndex = 2),
            RaftPeerProgress(nextIndex = 1, matchIndex = 0),
        )

        fixture.advancer.advance(peers)

        assertEquals(2L, fixture.volatileState.commitIndex)
    }

    @Test
    fun committingHigherIndexAdvancesDirectlyToThatIndex() = runTest {
        val fixture = fixture(
            currentTerm = 3,
            entryTerms = listOf(1, 1, 2, 2, 3),
        )
        val peers = listOf(
            RaftPeerProgress(nextIndex = 6, matchIndex = 5),
            RaftPeerProgress(nextIndex = 1, matchIndex = 0),
        )

        fixture.advancer.advance(peers)

        assertEquals(5L, fixture.volatileState.commitIndex)
    }

    private suspend fun fixture(
        currentTerm: Long,
        entryTerms: List<Long>,
    ): Fixture {
        val persistentState = RaftPersistentState(currentTerm = currentTerm)
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        for ((offset, term) in entryTerms.withIndex()) {
            val index = offset + 1L
            log.append(
                RaftLogEntry(
                    index = index,
                    term = term,
                    command = RaftCommand.Delete(byteArrayOf(index.toByte())),
                )
            )
        }
        return Fixture(
            volatileState = volatileState,
            advancer = RaftCommitAdvancer(
                persistentState = persistentState,
                volatileState = volatileState,
                log = log,
            ),
        )
    }

    private data class Fixture(
        val volatileState: RaftVolatileState,
        val advancer: RaftCommitAdvancer,
    )
}
