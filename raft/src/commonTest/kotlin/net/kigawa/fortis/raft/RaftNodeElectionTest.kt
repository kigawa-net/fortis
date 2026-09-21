package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RaftNodeElectionTest {
    @Test
    fun startElectionAdvancesTermAndVotesForSelf() = runTest {
        val fixture = fixture(currentTerm = 4)

        val request = fixture.node.startElection()

        assertEquals(5L, fixture.persistentState.currentTerm)
        assertEquals("self", fixture.persistentState.votedFor)
        assertEquals(RaftRole.CANDIDATE, fixture.node.role)
        assertEquals(5L, request.term)
        assertEquals("self", request.candidateId)
    }

    @Test
    fun requestVoteContainsLastLogPosition() = runTest {
        val fixture = fixture(currentTerm = 2)
        fixture.log.append(entry(index = 1, term = 1))
        fixture.log.append(entry(index = 2, term = 2))

        val request = fixture.node.startElection()

        assertEquals(2L, request.lastLogIndex)
        assertEquals(2L, request.lastLogTerm)
    }

    @Test
    fun majorityVoteMakesCandidateLeader() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.node.startElection()

        fixture.node.handleRequestVoteResponse(
            peerId = "peer-1",
            response = RequestVoteResponse(term = 2, voteGranted = true),
        )

        assertEquals(RaftRole.LEADER, fixture.node.role)
    }

    @Test
    fun rejectedVoteLeavesNodeAsCandidate() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.node.startElection()

        fixture.node.handleRequestVoteResponse(
            peerId = "peer-1",
            response = RequestVoteResponse(term = 2, voteGranted = false),
        )

        assertEquals(RaftRole.CANDIDATE, fixture.node.role)
    }

    @Test
    fun duplicateVoteIsCountedOnlyOnce() = runTest {
        val fixture = fixture(
            currentTerm = 1,
            peerIds = listOf("peer-1", "peer-2", "peer-3", "peer-4"),
        )
        fixture.node.startElection()
        val response = RequestVoteResponse(term = 2, voteGranted = true)

        fixture.node.handleRequestVoteResponse("peer-1", response)
        fixture.node.handleRequestVoteResponse("peer-1", response)

        assertEquals(RaftRole.CANDIDATE, fixture.node.role)
        fixture.node.handleRequestVoteResponse("peer-2", response)
        assertEquals(RaftRole.LEADER, fixture.node.role)
    }

    @Test
    fun higherTermResponseReturnsCandidateToFollower() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.node.startElection()

        fixture.node.handleRequestVoteResponse(
            peerId = "peer-1",
            response = RequestVoteResponse(term = 3, voteGranted = false),
        )

        assertEquals(3L, fixture.persistentState.currentTerm)
        assertNull(fixture.persistentState.votedFor)
        assertEquals(RaftRole.FOLLOWER, fixture.node.role)
    }

    @Test
    fun becomingLeaderInitializesPeerReplicationProgress() = runTest {
        val fixture = fixture(currentTerm = 1)
        fixture.log.append(entry(index = 1, term = 1))
        fixture.log.append(entry(index = 2, term = 1))
        fixture.node.startElection()

        fixture.node.handleRequestVoteResponse(
            peerId = "peer-1",
            response = RequestVoteResponse(term = 2, voteGranted = true),
        )

        for (progress in fixture.node.peers.values) {
            assertEquals(3L, progress.nextIndex)
            assertEquals(0L, progress.matchIndex)
        }
    }

    private fun fixture(
        currentTerm: Long,
        peerIds: List<String> = listOf("peer-1", "peer-2"),
    ): Fixture {
        val peers = peerIds.associateWith {
            RaftPeerProgress(nextIndex = 1)
        }.toMutableMap()
        val persistentState = RaftPersistentState(currentTerm = currentTerm)
        val log = MemoryRaftLog()
        return Fixture(
            persistentState = persistentState,
            peers = peers,
            log = log,
            node = RaftNodeBuilder(
                nodeId = "self",
                peers = peers,
                persistentState = persistentState,
                volatileState = RaftVolatileState(),
                log = log,
                stateMachine = RecordingStateMachine(),
            ).build(),
        )
    }

    private fun entry(index: Long, term: Long) =
        RaftLogEntry(
            index = index,
            term = term,
            command = RaftCommand.Delete(byteArrayOf(index.toByte())),
        )

    private data class Fixture(
        val persistentState: RaftPersistentState,
        val peers: MutableMap<String, RaftPeerProgress>,
        val log: MemoryRaftLog,
        val node: RaftNode,
    )

    private class RecordingStateMachine : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }
}
