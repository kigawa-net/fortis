package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.*

class RaftLeaderNoOpTest {
    @Test
    fun electionAppendsExactlyOneCurrentTermNoOpBeforeCommands() = runTest {
        val fixture = fixture()
        val candidate = assertIs<CandidateNode>(fixture.follower.onElectionTimeout().node)
        val leader = assertIs<LeaderNode>(candidate.handleRequestVoteResponse("peer-1", RequestVoteResponse(5, true)))
        assertEquals(RaftLogEntry(3, 5, RaftLogEntryPayload.NoOp), fixture.log.get(3))
        assertFalse(leader.isReady)
        assertEquals(0L, fixture.state.commitIndex)
        assertEquals(emptyList(), fixture.applied)
        leader.createAppendEntries("peer-1")
        leader.onHeartbeatTimeout()
        assertEquals(3L, fixture.log.lastIndex())
        assertEquals(4L, leader.appendCommand(command(4)).index)
    }

    @Test
    fun previousTermMajorityCannotCommitUntilCurrentTermNoOpIsReplicated() = runTest {
        val fixture = fixture()
        val leader = elect(fixture)
        val request = leader.createAppendEntries("peer-1")
        assertEquals(listOf(RaftLogEntry(3, 5, RaftLogEntryPayload.NoOp)), request.entries)
        // Acknowledgement of the prior entries alone is insufficient.
        leader.handleAppendEntriesResponse("peer-1", request.copy(entries = emptyList()), AppendEntriesResponse(5, true))
        assertEquals(2L, leader.peerProgress.getValue("peer-1").matchIndex)
        assertEquals(0L, fixture.state.commitIndex)
        assertFalse(leader.isReady)
        assertEquals(emptyList(), fixture.applied)
        val noOpRequest = leader.createAppendEntries("peer-1")
        leader.handleAppendEntriesResponse("peer-1", noOpRequest, AppendEntriesResponse(5, true))
        assertEquals(3L, fixture.state.commitIndex)
        assertEquals(3L, fixture.state.lastApplied)
        assertTrue(leader.isReady)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
    }

    @Test
    fun noOpWithoutMajorityDoesNotMakeLeaderReady() = runTest {
        val fixture = fixture()
        val leader = elect(fixture)
        leader.handleAppendEntriesResponse("peer-1", leader.createAppendEntries("peer-1"), AppendEntriesResponse(5, false))
        assertFalse(leader.isReady)
        assertEquals(0L, fixture.state.commitIndex)
        assertEquals(0L, fixture.state.lastApplied)
        assertEquals(emptyList(), fixture.applied)
    }

    @Test
    fun singleNodeCommitsAndAppliesNoOpWithoutCallingStateMachine() = runTest {
        val fixture = fixture(peers = emptySet(), previousEntries = false)
        val leader = assertIs<LeaderNode>(fixture.follower.onElectionTimeout().node)
        assertEquals(RaftLogEntry(1, 5, RaftLogEntryPayload.NoOp), fixture.log.get(1))
        assertEquals(1L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        assertTrue(leader.isReady)
        assertEquals(emptyList(), fixture.applied)
    }

    @Test
    fun followerAppliesCommandsAroundNoOpAndAdvancesLastAppliedOnce() = runTest {
        val fixture = fixture(previousEntries = false)
        val entries = listOf(
            RaftLogEntry(1, 5, command(1)),
            RaftLogEntry(2, 5, RaftLogEntryPayload.NoOp),
            RaftLogEntry(3, 5, command(3)),
        )
        val request = AppendEntriesRequest(5, "peer-1", 0, 0, entries, 3)
        assertTrue(fixture.follower.handleAppendEntries(request).value.success)
        assertTrue(fixture.follower.handleAppendEntries(request).value.success)
        assertEquals(3L, fixture.state.lastApplied)
        assertEquals(listOf(command(1), command(3)), fixture.applied)
    }

    @Test
    fun reelectionAppendsNewTermNoOpAndResetsReadiness() = runTest {
        val fixture = fixture()
        val leader = elect(fixture)
        leader.handleAppendEntriesResponse("peer-1", leader.createAppendEntries("peer-1"), AppendEntriesResponse(5, true))
        assertTrue(leader.isReady)
        val follower = assertIs<FollowerNode>(leader.handleAppendEntries(
            AppendEntriesRequest(6, "peer-1", 3, 5, emptyList(), 3),
        ).node)
        val candidate = assertIs<CandidateNode>(follower.onElectionTimeout().node)
        val next = assertIs<LeaderNode>(candidate.handleRequestVoteResponse("peer-1", RequestVoteResponse(7, true)))
        assertFalse(leader.isReady)
        assertFalse(next.isReady)
        assertEquals(RaftLogEntry(4, 7, RaftLogEntryPayload.NoOp), fixture.log.get(4))
        assertEquals(listOf(command(1), command(2)), fixture.applied)
        next.handleAppendEntriesResponse("peer-1", next.createAppendEntries("peer-1"), AppendEntriesResponse(7, true))
        assertTrue(next.isReady)
        assertEquals(4L, fixture.state.lastApplied)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
    }

    private suspend fun elect(fixture: Fixture): LeaderNode {
        val candidate = assertIs<CandidateNode>(fixture.follower.onElectionTimeout().node)
        return assertIs<LeaderNode>(candidate.handleRequestVoteResponse("peer-1", RequestVoteResponse(5, true)))
    }

    private suspend fun fixture(
        peers: Set<String> = setOf("peer-1", "peer-2"),
        previousEntries: Boolean = true,
    ): Fixture {
        val log = MemoryRaftLog()
        if (previousEntries) {
            log.append(RaftLogEntry(1, 3, command(1)))
            log.append(RaftLogEntry(2, 4, command(2)))
        }
        val state = RaftVolatileState()
        val applied = mutableListOf<RaftCommand>()
        val follower = RaftNodeBuilder(
            "self", peers, MemoryRaftPersistentStateStore(RaftPersistentState(4)), state, log,
            object : RaftStateMachine {
                override suspend fun apply(command: RaftCommand) { applied.add(command) }
            },
        ).build()
        return Fixture(follower, log, state, applied)
    }

    private data class Fixture(
        val follower: FollowerNode,
        val log: MemoryRaftLog,
        val state: RaftVolatileState,
        val applied: List<RaftCommand>,
    )

    private fun command(index: Int) = RaftCommand.Delete(byteArrayOf(index.toByte()))
}
