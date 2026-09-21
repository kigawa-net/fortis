package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals

class RaftRuntimeIntegrationTest {
    @Test
    fun threeNodesElectLeaderReplicateCommitAndApplyCommand() = runTest {
        val nodes = listOf("node-1", "node-2", "node-3").associateWith(::nodeFixture)
        val transport = LocalRaftTransport(nodes.mapValues { it.value.node })
        val leader = nodes.getValue("node-1")
        val runtime = RaftRuntime(
            node = leader.node,
            transport = transport,
            peerIds = setOf("node-2", "node-3"),
        )

        runtime.onElectionTimeout()

        assertEquals(RaftRole.LEADER, leader.node.role)
        val command = RaftCommand.Put(
            key = byteArrayOf(1),
            value = byteArrayOf(10),
        )
        runtime.appendCommand(command)

        assertEquals(1L, leader.volatileState.commitIndex)
        assertEquals(listOf<RaftCommand>(command), leader.stateMachine.applied)
        for (fixture in nodes.values) {
            assertEquals(command, fixture.log.get(1)?.command)
        }

        runtime.onHeartbeatTimeout()

        for (fixture in nodes.values) {
            assertEquals(1L, fixture.volatileState.commitIndex)
            assertEquals(1L, fixture.volatileState.lastApplied)
            assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
        }
    }

    private fun nodeFixture(nodeId: String): NodeFixture {
        val peerIds = setOf("node-1", "node-2", "node-3") - nodeId
        val persistentState = RaftPersistentState()
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        return NodeFixture(
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
            node = RaftNodeBuilder(
                nodeId = nodeId,
                peers = peerIds.associateWith {
                    RaftPeerProgress(nextIndex = 1)
                }.toMutableMap(),
                persistentState = persistentState,
                volatileState = volatileState,
                log = log,
                stateMachine = stateMachine,
            ).build(),
        )
    }

    private data class NodeFixture(
        val node: RaftNode,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
    )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
