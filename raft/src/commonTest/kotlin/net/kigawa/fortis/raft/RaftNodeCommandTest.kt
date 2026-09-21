package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftNodeCommandTest {
    @Test
    fun builderCreatesFollowerWithoutCommandApi() {
        assertIs<FollowerNode>(fixture().follower)
    }

    @Test
    fun leaderAppendsCommandWithCurrentTermAndNextIndex() = runTest {
        val fixture = electedLeader()
        val command = command()

        val entry = fixture.leader.appendCommand(command)

        assertEquals(1L, entry.index)
        assertEquals(1L, entry.term)
        assertEquals(command, entry.command)
        assertEquals(entry, fixture.log.get(1))
    }

    @Test
    fun majorityReplicationCommitsAndAppliesLeaderCommand() = runTest {
        val fixture = electedLeader()
        val command = command()
        fixture.leader.appendCommand(command)
        assertEquals(0L, fixture.volatileState.commitIndex)

        val request = fixture.leader.createAppendEntries("peer-1")
        val nextNode = fixture.leader.handleAppendEntriesResponse(
            "peer-1",
            request,
            AppendEntriesResponse(term = 1, success = true),
        )

        assertIs<LeaderNode>(nextNode)
        assertEquals(1L, fixture.volatileState.commitIndex)
        assertEquals(1L, fixture.volatileState.lastApplied)
        assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
    }

    @Test
    fun singleNodeLeaderCommitsAndAppliesCommandImmediately() = runTest {
        val fixture = fixture(peerIds = emptyList())
        val result = fixture.follower.onElectionTimeout()
        val leader = assertIs<LeaderNode>(result.node)
        val command = command()

        leader.appendCommand(command)

        assertEquals(1L, fixture.volatileState.commitIndex)
        assertEquals(1L, fixture.volatileState.lastApplied)
        assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
    }

    @Test
    fun higherTermAppendResponseReplacesLeaderWithFollower() = runTest {
        val fixture = electedLeader()
        val request = fixture.leader.createAppendEntries("peer-1")

        val nextNode = fixture.leader.handleAppendEntriesResponse(
            "peer-1",
            request,
            AppendEntriesResponse(term = 2, success = false),
        )

        assertIs<FollowerNode>(nextNode)
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

    private fun fixture(
        peerIds: List<String> = listOf("peer-1", "peer-2"),
    ): Fixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val follower = RaftNodeBuilder(
            nodeId = "self",
            peerIds = peerIds.toSet(),
            persistentState = RaftPersistentState(),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ).build()
        return Fixture(volatileState, log, stateMachine, follower)
    }

    private fun command() = RaftCommand.Put(
        key = byteArrayOf(1),
        value = byteArrayOf(10),
    )

    private class Fixture(
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
        val follower: FollowerNode,
    ) {
        lateinit var leader: LeaderNode
    }

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
