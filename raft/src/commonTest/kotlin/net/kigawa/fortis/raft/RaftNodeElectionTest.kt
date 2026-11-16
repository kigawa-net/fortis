package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class RaftNodeElectionTest {
    @Test
    fun startElectionReplacesFollowerWithCandidate() = runTest {
        val fixture = fixture(currentTerm = 4)

        val request = fixture.startElection()

        assertEquals(5L, fixture.persistentState.currentTerm)
        assertEquals("self", fixture.persistentState.votedFor)
        assertIs<CandidateNode>(fixture.node)
        assertEquals(5L, request.term)
        assertEquals("self", request.candidateId)
        assertEquals(
            RaftPersistentState(5, "self"),
            fixture.persistentStateStore.load(),
        )
    }

    @Test
    fun requestVoteContainsLastLogPosition() = runTest {
        val fixture = fixture(currentTerm = 2)
        fixture.log.append(entry(index = 1, term = 1))
        fixture.log.append(entry(index = 2, term = 2))

        val request = fixture.startElection()

        assertEquals(2L, request.lastLogIndex)
        assertEquals(2L, request.lastLogTerm)
    }

    @Test
    fun majorityVoteReplacesCandidateWithLeader() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.startElection()

        fixture.handleVote("peer-1", vote(term = 2))

        assertIs<LeaderNode>(fixture.node)
    }

    @Test
    fun rejectedVoteLeavesNodeAsCandidate() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.startElection()

        fixture.handleVote("peer-1", vote(term = 2, granted = false))

        assertIs<CandidateNode>(fixture.node)
    }

    @Test
    fun duplicateVoteIsCountedOnlyOnce() = runTest {
        val fixture = fixture(
            currentTerm = 1,
            peerIds = listOf("peer-1", "peer-2", "peer-3", "peer-4"),
        )
        fixture.startElection()

        fixture.handleVote("peer-1", vote(term = 2))
        fixture.handleVote("peer-1", vote(term = 2))

        assertIs<CandidateNode>(fixture.node)
        fixture.handleVote("peer-2", vote(term = 2))
        assertIs<LeaderNode>(fixture.node)
    }

    @Test
    fun higherTermResponseReplacesCandidateWithFollower() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.startElection()

        fixture.handleVote("peer-1", vote(term = 3, granted = false))

        assertEquals(3L, fixture.persistentState.currentTerm)
        assertNull(fixture.persistentState.votedFor)
        assertIs<FollowerNode>(fixture.node)
        assertEquals(
            RaftPersistentState(3, null),
            fixture.persistentStateStore.load(),
        )
    }

    @Test
    fun candidateElectionTimeoutPersistsNewTermAndSelfVote() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.startElection()
        val candidate = assertIs<CandidateNode>(fixture.node)

        val result = candidate.onElectionTimeout()

        assertEquals(3L, result.value.term)
        assertEquals(
            RaftPersistentState(3, "self"),
            fixture.persistentStateStore.load(),
        )
    }

    @Test
    fun persistenceFailurePreventsElectionStateChange() = runTest {
        val store = FailingStateStore(RaftPersistentState(4, null))
        val follower = RaftNodeBuilder(
            nodeId = "self",
            peerIds = setOf("peer-1"),
            persistentStateStore = store,
            volatileState = RaftVolatileState(),
            log = MemoryRaftLog(),
            stateMachine = NoOpStateMachine(),
        ).build()

        assertFailsWith<IllegalStateException> {
            follower.onElectionTimeout()
        }

        assertEquals(RaftPersistentState(4, null), follower.persistentState)
    }

    @Test
    fun builderRestoresTermAndVoteFromStore() = runTest {
        val fixture = fixture(currentTerm = 4)
        fixture.startElection()

        val rebuilt = RaftNodeBuilder(
            nodeId = "self",
            peerIds = setOf("peer-1", "peer-2"),
            persistentStateStore = fixture.persistentStateStore,
            volatileState = RaftVolatileState(),
            log = fixture.log,
            stateMachine = NoOpStateMachine(),
        ).build()

        assertEquals(5L, rebuilt.persistentState.currentTerm)
        assertEquals("self", rebuilt.persistentState.votedFor)
    }

    @Test
    fun validAppendEntriesReplacesCandidateWithFollower() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.startElection()
        val candidate = assertIs<CandidateNode>(fixture.node)

        val result = candidate.handleAppendEntries(
            AppendEntriesRequest(
                term = 2,
                leaderId = "peer-1",
                prevLogIndex = 0,
                prevLogTerm = 0,
                entries = emptyList(),
                leaderCommit = 0,
            ),
        )

        assertIs<FollowerNode>(result.node)
    }

    @Test
    fun becomingLeaderInitializesPeerReplicationProgress() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.log.append(entry(index = 1, term = 1))
        fixture.log.append(entry(index = 2, term = 1))
        fixture.startElection()

        fixture.handleVote("peer-1", vote(term = 2))

        val leader = assertIs<LeaderNode>(fixture.node)
        for (progress in leader.peerProgress.values) {
            assertEquals(3L, progress.nextIndex)
            assertEquals(0L, progress.matchIndex)
        }
    }

    private suspend fun fixture(
        currentTerm: Long,
        peerIds: List<String> = listOf("peer-1", "peer-2"),
    ): Fixture {
        val persistentStateStore = MemoryRaftPersistentStateStore(
            RaftPersistentState(currentTerm = currentTerm),
        )
        val log = MemoryRaftLog()
        val node = RaftNodeBuilder(
            nodeId = "self",
            peerIds = peerIds.toSet(),
            persistentStateStore = persistentStateStore,
            volatileState = RaftVolatileState(),
            log = log,
            stateMachine = NoOpStateMachine(),
        ).build()
        return Fixture(node.persistentState, persistentStateStore, log, node)
    }

    private fun vote(term: Long, granted: Boolean = true) =
        RequestVoteResponse(term, granted)

    private fun entry(index: Long, term: Long) = RaftLogEntry(
        index,
        term,
        RaftCommand.Delete(byteArrayOf(index.toByte())),
    )

    private data class Fixture(
        val persistentState: RaftPersistentState,
        val persistentStateStore: RaftPersistentStateStore,
        val log: MemoryRaftLog,
        var node: RaftNode,
    ) {
        suspend fun startElection(): RequestVoteRequest {
            val result = assertIs<FollowerNode>(node).onElectionTimeout()
            node = result.node
            return result.value
        }

        suspend fun handleVote(peerId: String, response: RequestVoteResponse) {
            node = assertIs<CandidateNode>(node)
                .handleRequestVoteResponse(peerId, response)
        }
    }

    private class NoOpStateMachine : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }

    private class FailingStateStore(
        private val state: RaftPersistentState,
    ) : RaftPersistentStateStore {
        override suspend fun load(): RaftPersistentState = state.copy()

        override suspend fun save(term: Long, votedFor: String?): Nothing {
            error("save failed")
        }
    }
}
