package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftRuntimeIntegrationTest {
    @Test
    fun threeNodesElectLeaderReplicateCommitAndApplyCommand() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")

        leader.runtime.onElectionTimeout()

        assertIs<LeaderNode>(leader.runtime.currentNode)
        val command = RaftCommand.Put(
            key = byteArrayOf(1),
            value = byteArrayOf(10),
        )
        leader.runtime.appendCommand(command)

        assertEquals(1L, leader.volatileState.commitIndex)
        assertEquals(listOf<RaftCommand>(command), leader.stateMachine.applied)
        for (fixture in cluster.nodes.values) {
            assertEquals(command, fixture.log.get(1)?.command)
        }

        leader.runtime.onHeartbeatTimeout()

        for (fixture in cluster.nodes.values) {
            assertEquals(1L, fixture.volatileState.commitIndex)
            assertEquals(1L, fixture.volatileState.lastApplied)
            assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
        }
    }

    @Test
    fun replicationRetriesUntilBehindFollowerCatchesUp() = runTest {
        val cluster = cluster()
        appendTerms(cluster.nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(cluster.nodes.getValue("node-2").log, 1)
        appendTerms(cluster.nodes.getValue("node-3").log, 1, 2, 2)
        val leader = cluster.nodes.getValue("node-1").runtime

        leader.onElectionTimeout()
        cluster.transport.appendRequests.clear()
        leader.replicate()

        assertEquals(
            listOf(3L, 2L, 1L),
            cluster.transport.appendRequests
                .getValue("node-2")
                .map { it.prevLogIndex },
        )
        assertLogTerms(cluster.nodes.getValue("node-2").log, 1, 2, 2)
    }

    @Test
    fun replicationReplacesConflictingFollowerEntries() = runTest {
        val cluster = cluster()
        appendTerms(cluster.nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(cluster.nodes.getValue("node-2").log, 1, 1, 1)
        appendTerms(cluster.nodes.getValue("node-3").log, 1, 2, 2)
        val leader = cluster.nodes.getValue("node-1").runtime

        leader.onElectionTimeout()
        cluster.transport.appendRequests.clear()
        leader.replicate()

        assertEquals(
            listOf(3L, 2L, 1L),
            cluster.transport.appendRequests
                .getValue("node-2")
                .map { it.prevLogIndex },
        )
        assertLogTerms(cluster.nodes.getValue("node-2").log, 1, 2, 2)
    }

    @Test
    fun heartbeatRetriesUntilBehindFollowerCatchesUp() = runTest {
        val cluster = cluster()
        appendTerms(cluster.nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(cluster.nodes.getValue("node-2").log, 1)
        appendTerms(cluster.nodes.getValue("node-3").log, 1, 2, 2)
        val leader = cluster.nodes.getValue("node-1").runtime

        leader.onElectionTimeout()
        cluster.transport.appendRequests.clear()
        leader.onHeartbeatTimeout()

        assertEquals(
            listOf(3L, 2L, 1L),
            cluster.transport.appendRequests
                .getValue("node-2")
                .map { it.prevLogIndex },
        )
        assertLogTerms(cluster.nodes.getValue("node-2").log, 1, 2, 2)
    }

    private fun cluster(): Cluster {
        val localTransport = LocalRaftTransport()
        val recordingTransport = RecordingTransport(localTransport)
        val nodes = listOf("node-1", "node-2", "node-3").associateWith { nodeId ->
            nodeFixture(nodeId, recordingTransport)
        }
        for ((nodeId, fixture) in nodes) {
            localTransport.register(nodeId, fixture.runtime)
        }
        return Cluster(nodes, recordingTransport)
    }

    private fun nodeFixture(
        nodeId: String,
        transport: RaftTransport,
    ): NodeFixture {
        val peerIds = setOf("node-1", "node-2", "node-3") - nodeId
        val persistentState = RaftPersistentState()
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val follower = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = peerIds,
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ).build()
        return NodeFixture(
            runtime = RaftRuntime(follower, transport),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        )
    }

    private suspend fun appendTerms(log: MemoryRaftLog, vararg terms: Long) {
        terms.forEachIndexed { offset, term ->
            val index = offset + 1L
            log.append(
                RaftLogEntry(
                    index = index,
                    term = term,
                    command = RaftCommand.Delete(byteArrayOf(index.toByte())),
                ),
            )
        }
    }

    private suspend fun assertLogTerms(log: MemoryRaftLog, vararg terms: Long) {
        assertEquals(terms.size.toLong(), log.lastIndex())
        terms.forEachIndexed { offset, term ->
            assertEquals(term, log.get(offset + 1L)?.term)
        }
    }

    private data class Cluster(
        val nodes: Map<String, NodeFixture>,
        val transport: RecordingTransport,
    )

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

    private class RecordingTransport(
        private val delegate: RaftTransport,
    ) : RaftTransport {
        val appendRequests = mutableMapOf<String, MutableList<AppendEntriesRequest>>()

        override suspend fun requestVote(
            peerId: String,
            request: RequestVoteRequest,
        ): RequestVoteResponse = delegate.requestVote(peerId, request)

        override suspend fun appendEntries(
            peerId: String,
            request: AppendEntriesRequest,
        ): AppendEntriesResponse {
            appendRequests.getOrPut(peerId) { mutableListOf() }.add(request)
            return delegate.appendEntries(peerId, request)
        }
    }
}
