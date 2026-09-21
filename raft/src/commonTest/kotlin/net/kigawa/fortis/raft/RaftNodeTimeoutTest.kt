package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RaftNodeTimeoutTest {
    @Test
    fun electionTimeoutCreatesCandidateAndResetsElectionTimer() = runTest {
        val fixture = fixture()

        val result = fixture.follower.onElectionTimeout()

        assertIs<CandidateNode>(result.node)
        assertEquals(1L, result.value.term)
        assertEquals("self", result.value.candidateId)
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    @Test
    fun heartbeatTimeoutReplicatesPendingEntriesForEveryPeer() = runTest {
        val fixture = electedLeader()
        fixture.leader.appendCommand(command())
        fixture.timer.events.clear()

        val heartbeats = fixture.leader.onHeartbeatTimeout()

        assertEquals(setOf("peer-1", "peer-2"), heartbeats.keys)
        assertTrue(heartbeats.values.all { it.entries.size == 1 })
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Heartbeat),
            fixture.timer.events,
        )
    }

    @Test
    fun followerDoesNotExposeHeartbeatApi() = runTest {
        assertIs<FollowerNode>(fixture().follower)
    }

    @Test
    fun appendEntriesFromCurrentLeaderResetsElectionTimer() = runTest {
        val fixture = fixture()

        fixture.follower.handleAppendEntries(appendEntries(term = 1))

        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    @Test
    fun staleAppendEntriesDoesNotResetElectionTimer() = runTest {
        val fixture = fixture(currentTerm = 2)

        fixture.follower.handleAppendEntries(appendEntries(term = 1))

        assertEquals(emptyList<RaftTimeoutEvent>(), fixture.timer.events)
    }

    @Test
    fun grantingVoteResetsElectionTimer() = runTest {
        val fixture = fixture()

        val result = fixture.follower.handleRequestVote(
            RequestVoteRequest(
                term = 1,
                candidateId = "peer-1",
                lastLogIndex = 0,
                lastLogTerm = 0,
            ),
        )

        assertTrue(result.value.voteGranted)
        assertEquals(
            listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election),
            fixture.timer.events,
        )
    }

    private suspend fun electedLeader(): Fixture {
        val fixture = fixture()
        val election = fixture.follower.onElectionTimeout()
        val candidate = assertIs<CandidateNode>(election.node)
        fixture.leader = assertIs<LeaderNode>(
            candidate.handleRequestVoteResponse(
                "peer-1",
                RequestVoteResponse(term = 1, voteGranted = true),
            ),
        )
        return fixture
    }

    private suspend fun fixture(currentTerm: Long = 0): Fixture {
        val timer = RecordingTimer()
        val follower = RaftNodeBuilder(
            nodeId = "self",
            peerIds = setOf("peer-1", "peer-2"),
            persistentStateStore = MemoryRaftPersistentStateStore(
                RaftPersistentState(currentTerm = currentTerm),
            ),
            volatileState = RaftVolatileState(),
            log = MemoryRaftLog(),
            stateMachine = NoOpStateMachine(),
            timer = timer,
        ).build()
        return Fixture(follower, timer)
    }

    private fun appendEntries(term: Long) = AppendEntriesRequest(
        term = term,
        leaderId = "leader",
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = emptyList(),
        leaderCommit = 0,
    )

    private fun command() = RaftCommand.Put(
        key = byteArrayOf(1),
        value = byteArrayOf(10),
    )

    private class Fixture(
        val follower: FollowerNode,
        val timer: RecordingTimer,
    ) {
        lateinit var leader: LeaderNode
    }

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
