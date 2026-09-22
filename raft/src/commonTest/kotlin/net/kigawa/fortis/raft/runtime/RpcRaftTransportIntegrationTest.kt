package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.InMemoryRaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RpcRaftTransportIntegrationTest {
    @Test
    fun threeNodesElectReplicateAndApplyThroughRpcFrames() = runTest {
        val channel = RecordingChannel(InMemoryRaftRpcChannel())
        val transport = RpcRaftTransport(channel)
        val nodes = listOf("node-1", "node-2", "node-3").associateWith { nodeId ->
            node(nodeId, transport)
        }
        for ((nodeId, fixture) in nodes) {
            channel.delegate.register(nodeId, RaftRpcHandler(fixture.runtime))
        }
        val leader = nodes.getValue("node-1")

        leader.runtime.onElectionTimeout()

        assertIs<LeaderNode>(leader.runtime.currentNode)
        val command = RaftCommand.Put(
            key = byteArrayOf(1, 2),
            value = byteArrayOf(10, 20),
        )
        leader.runtime.appendCommand(command)
        leader.runtime.onHeartbeatTimeout()

        assertTrue(channel.frames.all { it.copyOfRange(0, 4).contentEquals("FRPC".encodeToByteArray()) })
        assertTrue(channel.frames.size >= 10)
        for (fixture in nodes.values) {
            val applied = assertIs<RaftCommand.Put>(fixture.stateMachine.applied.single())
            assertContentEquals(command.key, applied.key)
            assertContentEquals(command.value, applied.value)
            assertEquals(1L, fixture.volatileState.commitIndex)
            assertEquals(1L, fixture.volatileState.lastApplied)
            assertEquals(command, fixture.log.get(1)?.command)
        }
    }

    private suspend fun node(
        nodeId: String,
        transport: RpcRaftTransport,
    ): NodeFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val initialNode = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = setOf("node-1", "node-2", "node-3") - nodeId,
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ).build()
        return NodeFixture(
            runtime = RaftRuntime(initialNode, transport),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        )
    }

    private data class NodeFixture(
        val runtime: RaftRuntime,
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

    private class RecordingChannel(
        val delegate: InMemoryRaftRpcChannel,
    ) : RaftRpcChannel {
        val frames = mutableListOf<ByteArray>()

        override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
            frames.add(frame.copyOf())
            val response = delegate.request(peerId, frame)
            frames.add(response.copyOf())
            return response
        }
    }
}
