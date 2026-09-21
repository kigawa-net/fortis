package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RaftNodeTimeoutTest {
    @Test
    fun electionTimeoutStartsElectionAndResetsElectionTimer() = runTest {
        val fixture = fixture()

        val request = fixture.node.onElectionTimeout()

        assertEquals(RaftRole.CANDIDATE, fixture.node.role)
        assertEquals(1L, request.term)
        assertEquals("self", request.candidateId)
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    @Test
    fun heartbeatTimeoutCreatesEmptyAppendEntriesForEveryPeer() = runTest {
        val fixture = electedLeader()
        fixture.node.appendCommand(command())
        fixture.timer.events.clear()

        val heartbeats = fixture.node.onHeartbeatTimeout()

        assertEquals(setOf("peer-1", "peer-2"), heartbeats.keys)
        assertTrue(heartbeats.values.all { it.entries.isEmpty() })
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Heartbeat),
            fixture.timer.events,
        )
    }

    @Test
    fun heartbeatTimeoutIsRejectedWhenNotLeader() = runTest {
        val fixture = fixture()

        assertFailsWith<IllegalStateException> {
            fixture.node.onHeartbeatTimeout()
        }
    }

    @Test
    fun appendEntriesFromCurrentLeaderResetsElectionTimer() = runTest {
        val fixture = fixture()

        fixture.node.handleAppendEntries(appendEntries(term = 1))

        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    @Test
    fun staleAppendEntriesDoesNotResetElectionTimer() = runTest {
        val fixture = fixture(currentTerm = 2)

        fixture.node.handleAppendEntries(appendEntries(term = 1))

        assertEquals(emptyList<RaftTimeoutEvent>(), fixture.timer.events)
    }

    @Test
    fun grantingVoteResetsElectionTimer() = runTest {
        val fixture = fixture()

        val response = fixture.node.handleRequestVote(
            RequestVoteRequest(
                term = 1,
                candidateId = "peer-1",
                lastLogIndex = 0,
                lastLogTerm = 0,
            )
        )

        assertTrue(response.voteGranted)
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    private suspend fun electedLeader(): Fixture =
        fixture().also { fixture ->
            fixture.node.startElection()
            fixture.node.handleRequestVoteResponse(
                peerId = "peer-1",
                response = RequestVoteResponse(term = 1, voteGranted = true),
            )
            assertEquals(RaftRole.LEADER, fixture.node.role)
        }

    private fun fixture(
        currentTerm: Long = 0,
    ): Fixture {
        val peers = mutableMapOf(
            "peer-1" to RaftPeerProgress(nextIndex = 1),
            "peer-2" to RaftPeerProgress(nextIndex = 1),
        )
        val timer = RecordingTimer()
        return Fixture(
            timer = timer,
            node = RaftNodeBuilder(
                nodeId = "self",
                peers = peers,
                persistentState = RaftPersistentState(currentTerm = currentTerm),
                volatileState = RaftVolatileState(),
                log = MemoryRaftLog(),
                stateMachine = NoOpStateMachine(),
                timer = timer,
            ).build(),
        )
    }

    private fun appendEntries(term: Long) =
        AppendEntriesRequest(
            term = term,
            leaderId = "leader",
            prevLogIndex = 0,
            prevLogTerm = 0,
            entries = emptyList(),
            leaderCommit = 0,
        )

    private fun command() =
        RaftCommand.Put(
            key = byteArrayOf(1),
            value = byteArrayOf(10),
        )

    private data class Fixture(
        val node: RaftNode,
        val timer: RecordingTimer,
    )

    private class RecordingTimer : RaftTimer {
        val events = mutableListOf<RaftTimeoutEvent>()

        override suspend fun reset(event: RaftTimeoutEvent) {
            events.add(event)
        }
    }

    private class NoOpStateMachine : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }
}
