package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
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

    @Test
    fun replicationRetriesUntilBehindFollowerCatchesUp() = runTest {
        val nodes = listOf("node-1", "node-2", "node-3").associateWith(::nodeFixture)
        appendTerms(nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(nodes.getValue("node-2").log, 1)
        appendTerms(nodes.getValue("node-3").log, 1, 2, 2)
        val transport = RecordingTransport(
            LocalRaftTransport(nodes.mapValues { it.value.node }),
        )
        val runtime = runtime(nodes.getValue("node-1").node, transport)

        runtime.onElectionTimeout()
        transport.appendRequests.clear()
        runtime.replicate()

        assertEquals(
            listOf(3L, 2L, 1L),
            transport.appendRequests.getValue("node-2").map { it.prevLogIndex },
        )
        assertLogTerms(nodes.getValue("node-2").log, 1, 2, 2)
    }

    @Test
    fun replicationReplacesConflictingFollowerEntries() = runTest {
        val nodes = listOf("node-1", "node-2", "node-3").associateWith(::nodeFixture)
        appendTerms(nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(nodes.getValue("node-2").log, 1, 1, 1)
        appendTerms(nodes.getValue("node-3").log, 1, 2, 2)
        val transport = RecordingTransport(
            LocalRaftTransport(nodes.mapValues { it.value.node }),
        )
        val runtime = runtime(nodes.getValue("node-1").node, transport)

        runtime.onElectionTimeout()
        transport.appendRequests.clear()
        runtime.replicate()

        assertEquals(
            listOf(3L, 2L, 1L),
            transport.appendRequests.getValue("node-2").map { it.prevLogIndex },
        )
        assertLogTerms(nodes.getValue("node-2").log, 1, 2, 2)
    }

    @Test
    fun heartbeatRetriesUntilBehindFollowerCatchesUp() = runTest {
        val nodes = listOf("node-1", "node-2", "node-3").associateWith(::nodeFixture)
        appendTerms(nodes.getValue("node-1").log, 1, 2, 2)
        appendTerms(nodes.getValue("node-2").log, 1)
        appendTerms(nodes.getValue("node-3").log, 1, 2, 2)
        val transport = RecordingTransport(
            LocalRaftTransport(nodes.mapValues { it.value.node }),
        )
        val runtime = runtime(nodes.getValue("node-1").node, transport)

        runtime.onElectionTimeout()
        transport.appendRequests.clear()
        runtime.onHeartbeatTimeout()

        assertEquals(
            listOf(3L, 2L, 1L),
            transport.appendRequests.getValue("node-2").map { it.prevLogIndex },
        )
        assertLogTerms(nodes.getValue("node-2").log, 1, 2, 2)
    }

    private fun runtime(node: RaftNode, transport: RaftTransport) = RaftRuntime(
        node = node,
        transport = transport,
        peerIds = setOf("node-2", "node-3"),
    )

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
