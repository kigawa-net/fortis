package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RaftNodeCommandTest {
    @Test
    fun followerCannotAppendCommand() = runTest {
        val fixture = fixture()

        assertFailsWith<IllegalStateException> {
            fixture.node.appendCommand(command())
        }

        assertEquals(0L, fixture.log.lastIndex())
    }

    @Test
    fun leaderAppendsCommandWithCurrentTermAndNextIndex() = runTest {
        val fixture = electedLeader()
        val command = command()

        val entry = fixture.node.appendCommand(command)

        assertEquals(1L, entry.index)
        assertEquals(1L, entry.term)
        assertEquals(command, entry.command)
        assertEquals(entry, fixture.log.get(1))
    }

    @Test
    fun majorityReplicationCommitsAndAppliesLeaderCommand() = runTest {
        val fixture = electedLeader()
        val command = command()
        fixture.node.appendCommand(command)
        assertEquals(0L, fixture.volatileState.commitIndex)
        assertEquals(emptyList(), fixture.stateMachine.applied)

        val request = fixture.node.createAppendEntries("peer-1")
        fixture.node.handleAppendEntriesResponse(
            peerId = "peer-1",
            request = request,
            response = AppendEntriesResponse(term = 1, success = true),
        )

        assertEquals(1L, fixture.volatileState.commitIndex)
        assertEquals(1L, fixture.volatileState.lastApplied)
        assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
    }

    @Test
    fun singleNodeLeaderCommitsAndAppliesCommandImmediately() = runTest {
        val fixture = fixture(peerIds = emptyList())
        fixture.node.startElection()
        val command = command()

        fixture.node.appendCommand(command)

        assertEquals(RaftRole.LEADER, fixture.node.role)
        assertEquals(1L, fixture.volatileState.commitIndex)
        assertEquals(1L, fixture.volatileState.lastApplied)
        assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
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
        peerIds: List<String> = listOf("peer-1", "peer-2"),
    ): Fixture {
        val peers = peerIds.associateWith {
            RaftPeerProgress(nextIndex = 1)
        }.toMutableMap()
        val persistentState = RaftPersistentState()
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        return Fixture(
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
            node = RaftNodeBuilder(
                nodeId = "self",
                peers = peers,
                persistentState = persistentState,
                volatileState = volatileState,
                log = log,
                stateMachine = stateMachine,
            ).build(),
        )
    }

    private fun command() =
        RaftCommand.Put(
            key = byteArrayOf(1),
            value = byteArrayOf(10),
        )

    private data class Fixture(
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
        val node: RaftNode,
    )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
