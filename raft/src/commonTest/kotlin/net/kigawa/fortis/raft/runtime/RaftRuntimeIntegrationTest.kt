package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftRuntimeIntegrationTest {
    @Test
    fun oneUnavailablePeerDoesNotPreventElection() = runTest {
        val cluster = cluster()
        cluster.transport.unavailablePeers.add("node-2")

        cluster.nodes.getValue("node-1").runtime.onElectionTimeout()

        assertIs<LeaderNode>(cluster.nodes.getValue("node-1").runtime.currentNode)
        assertEquals(listOf("node-2", "node-3"), cluster.transport.voteAttempts)
    }

    @Test
    fun oneUnavailablePeerStillAllowsMajorityCommit() = runTest {
        val cluster = cluster()
        cluster.transport.unavailablePeers.add("node-2")
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))

        leader.runtime.appendCommand(command)

        assertEquals(1L, leader.volatileState.commitIndex)
        assertEquals(listOf<RaftCommand>(command), leader.stateMachine.applied)
        assertEquals(0L, cluster.nodes.getValue("node-2").log.lastIndex())
        assertEquals(command, cluster.nodes.getValue("node-3").log.get(1)?.command)
    }

    @Test
    fun twoUnavailablePeersAppendLocallyWithoutCommit() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        cluster.transport.unavailablePeers.addAll(listOf("node-2", "node-3"))
        val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))

        leader.runtime.appendCommand(command)

        assertIs<LeaderNode>(leader.runtime.currentNode)
        assertEquals(command, leader.log.get(1)?.command)
        assertEquals(0L, leader.volatileState.commitIndex)
        assertEquals(0L, leader.volatileState.lastApplied)
        assertEquals(emptyList(), leader.stateMachine.applied)
    }

    @Test
    fun unreachableHeartbeatPeerDoesNotDemoteLeader() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        cluster.transport.unavailablePeers.add("node-2")
        cluster.transport.appendAttempts.clear()

        leader.runtime.onHeartbeatTimeout()

        assertIs<LeaderNode>(leader.runtime.currentNode)
        assertEquals(listOf("node-2", "node-3"), cluster.transport.appendAttempts)
    }

    @Test
    fun recoveredFollowerCatchesUpOnHeartbeat() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        val recovered = cluster.nodes.getValue("node-3")
        leader.runtime.onElectionTimeout()
        cluster.transport.unavailablePeers.add("node-3")
        val commands: List<RaftCommand> = (1..3).map { value ->
            RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf((value * 10).toByte()))
        }
        commands.forEach { leader.runtime.appendCommand(it) }

        assertEquals(3L, leader.volatileState.commitIndex)
        assertEquals(0L, recovered.log.lastIndex())

        cluster.transport.unavailablePeers.remove("node-3")
        leader.runtime.onHeartbeatTimeout()

        assertEquals(3L, recovered.log.lastIndex())
        assertEquals(commands, (1L..3L).map { recovered.log.get(it)?.command })
        assertEquals(3L, recovered.volatileState.commitIndex)
        assertEquals(3L, recovered.volatileState.lastApplied)
        assertEquals(commands, recovered.stateMachine.applied)
    }

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

    private suspend fun cluster(): Cluster {
        val localTransport = LocalRaftTransport()
        val recordingTransport = RecordingTransport(localTransport)
        val nodes = mutableMapOf<String, NodeFixture>()
        for (nodeId in listOf("node-1", "node-2", "node-3")) {
            nodes[nodeId] = nodeFixture(nodeId, recordingTransport)
        }
        for ((nodeId, fixture) in nodes) {
            localTransport.register(nodeId, fixture.runtime)
        }
        return Cluster(nodes, recordingTransport)
    }

    private suspend fun nodeFixture(
        nodeId: String,
        transport: RaftTransport,
    ): NodeFixture {
        val peerIds = setOf("node-1", "node-2", "node-3") - nodeId
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val follower = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = peerIds,
            persistentStateStore = MemoryRaftPersistentStateStore(),
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

    private class RecordingStateMachine: RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }

    private class RecordingTransport(
        private val delegate: RaftTransport,
    ): RaftTransport {
        val unavailablePeers = mutableSetOf<String>()
        val voteAttempts = mutableListOf<String>()
        val appendAttempts = mutableListOf<String>()
        val appendRequests = mutableMapOf<String, MutableList<AppendEntriesRequest>>()

        override suspend fun requestVote(
            peerId: String,
            request: RequestVoteRequest,
        ): RequestVoteResponse {
            voteAttempts.add(peerId)
            if (peerId in unavailablePeers) {
                throw RaftPeerUnavailableException(peerId)
            }
            return delegate.requestVote(peerId, request)
        }

        override suspend fun appendEntries(
            peerId: String,
            request: AppendEntriesRequest,
        ): AppendEntriesResponse {
            appendAttempts.add(peerId)
            appendRequests.getOrPut(peerId) { mutableListOf() }.add(request)
            if (peerId in unavailablePeers) {
                throw RaftPeerUnavailableException(peerId)
            }
            return delegate.appendEntries(peerId, request)
        }
    }
}
