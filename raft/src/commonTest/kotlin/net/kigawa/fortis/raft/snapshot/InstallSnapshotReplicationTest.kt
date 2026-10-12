package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

// Runtime 経由の InstallSnapshot 受付と leader→follower 追従を確認する
class InstallSnapshotReplicationTest {
    @Test
    fun runtimeAppliesFreshSnapshot() = runTest {
        val fixture = followerFixture(term = 2)
        fixture.appendEntries(1L..3L, term = 2)

        val response = fixture.runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(3, 3),
                data = byteArrayOf(9),
            ),
        )

        assertEquals(3, response.term)
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(0, fixture.log.lastIndex())
        assertEquals(3, fixture.volatileState.commitIndex)
        assertEquals(3, fixture.volatileState.lastApplied)
        assertEquals(1, fixture.applied.size)
        assertEquals(3L, fixture.applied.single().first.lastIncludedIndex)
        assertEquals(1, fixture.timerResets())
    }

    @Test
    fun runtimeIgnoresStaleSnapshot() = runTest {
        val fixture = followerFixture(term = 3)
        fixture.runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(5, 3),
                data = byteArrayOf(1),
            ),
        )
        fixture.appendEntries(1L..1L, term = 3)

        val response = fixture.runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(2, 3),
                data = byteArrayOf(2),
            ),
        )

        assertEquals(3, response.term)
        assertEquals(1, fixture.log.lastIndex())
        assertEquals(5, fixture.volatileState.commitIndex)
        assertEquals(1, fixture.applied.size)
    }

    @Test
    fun runtimeRejectsLowerTermWithoutSideEffects() = runTest {
        val fixture = followerFixture(term = 4)
        fixture.appendEntries(1L..2L, term = 4)

        val response = fixture.runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(2, 4),
                data = byteArrayOf(1),
            ),
        )

        assertEquals(4, response.term)
        assertEquals(2, fixture.log.lastIndex())
        assertEquals(0, fixture.volatileState.commitIndex)
        assertTrue(fixture.applied.isEmpty())
        assertEquals(0, fixture.timerResets())
    }

    @Test
    fun runtimeStepsDownLeaderOnHigherTermSnapshot() = runTest {
        val local = LocalRaftTransport()
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val follower = RaftNodeBuilder(
            nodeId = "node-1",
            peerIds = emptySet(),
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = RecordingStateMachine(),
        ).build()
        val runtime = RaftRuntime(follower, local)
        local.register("node-1", runtime)
        runtime.onElectionTimeout()
        assertIs<LeaderNode>(runtime.currentNode)

        val response = runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 2,
                leaderId = "leader-9",
                metadata = SnapshotMetadata(1, 2),
                data = byteArrayOf(5),
            ),
        )

        assertEquals(2, response.term)
        assertIs<FollowerNode>(runtime.currentNode)
    }

    @Test
    fun laggingFollowerCatchesUpThroughSnapshot() = runTest {
        val gate = GateTransport(LocalRaftTransport())
        val nodes = mutableMapOf<String, NodeFixture>()
        for (nodeId in listOf("node-1", "node-2", "node-3")) {
            val fixture = nodeFixture(nodeId, gate, provider = nodeId == "node-1")
            nodes[nodeId] = fixture
            gate.delegate.register(nodeId, fixture.runtime)
        }
        val leader = nodes.getValue("node-1")
        val lagging = nodes.getValue("node-3")

        leader.runtime.onElectionTimeout()
        gate.blocked.add("node-3")
        val commands: List<RaftCommand> = (1..4).map { value ->
            RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf((value * 10).toByte()))
        }
        commands.forEach { leader.runtime.appendCommand(it) }
        (leader.runtime.currentNode as LeaderNode)
            .updateSnapshotMetadata(SnapshotMetadata(3, 1))
        gate.blocked.remove("node-3")

        leader.runtime.replicate()

        assertEquals(listOf("node-3"), gate.snapshotTargets)
        val progress = (leader.runtime.currentNode as LeaderNode).peerProgress.getValue("node-3")
        assertEquals(6, progress.nextIndex)
        assertEquals(5, progress.matchIndex)
        assertEquals(5, lagging.log.lastIndex())
        assertEquals(commands, (2L..5L).map { lagging.log.get(it)?.command })
        assertEquals(5, lagging.volatileState.commitIndex)
        assertEquals(5, lagging.volatileState.lastApplied)
        // idx1..3 はスナップショット側で吸収されたため、状態機械には idx4..5 のみ適用される
        assertEquals(commands.drop(2), lagging.stateMachine.applied)
        assertEquals(1, lagging.applied.size)
        val (appliedMetadata, appliedData) = lagging.applied.single()
        assertEquals(SnapshotMetadata(3, 1), appliedMetadata)
        assertTrue(appliedData.contentEquals(byteArrayOf(7)))
    }

    private suspend fun followerFixture(term: Long): FollowerFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val applied = mutableListOf<Pair<SnapshotMetadata, ByteArray>>()
        var timerResets = 0
        val follower = RaftNodeBuilder(
            nodeId = "node-9",
            peerIds = setOf("leader-1"),
            persistentStateStore = MemoryRaftPersistentStateStore(RaftPersistentState(term)),
            volatileState = volatileState,
            log = log,
            stateMachine = RecordingStateMachine(),
            timer = RaftTimer { event ->
                if (event == RaftTimeoutEvent.Election) timerResets++
            },
        ).build()
        val runtime = RaftRuntime(
            follower,
            LocalRaftTransport(),
            snapshotApplier = RaftSnapshotApplier { metadata, data ->
                applied.add(metadata to data.copyOf())
            },
        )
        return FollowerFixture(runtime, volatileState, log, applied, { timerResets })
    }

    private suspend fun nodeFixture(
        nodeId: String,
        transport: RaftTransport,
        provider: Boolean,
    ): NodeFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val applied = mutableListOf<Pair<SnapshotMetadata, ByteArray>>()
        val follower = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = setOf("node-1", "node-2", "node-3") - nodeId,
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ).build()
        val runtime = RaftRuntime(
            follower,
            transport,
            snapshotApplier = RaftSnapshotApplier { metadata, data ->
                applied.add(metadata to data.copyOf())
            },
            snapshotProvider = if (provider) {
                RaftSnapshotProvider { byteArrayOf(7) }
            } else {
                null
            },
        )
        return NodeFixture(runtime, volatileState, log, stateMachine, applied)
    }

    private data class FollowerFixture(
        val runtime: RaftRuntime,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val applied: List<Pair<SnapshotMetadata, ByteArray>>,
        val timerResets: () -> Int,
    ) {
        suspend fun appendEntries(indexes: LongRange, term: Long) {
            for (index in indexes) {
                log.append(
                    RaftLogEntry(
                        index = index,
                        term = term,
                        command = RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(1)),
                    ),
                )
            }
        }
    }

    private data class NodeFixture(
        val runtime: RaftRuntime,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
        val applied: List<Pair<SnapshotMetadata, ByteArray>>,
    )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }

    /** 遅れ再現のため特定 peer への送信を遮断できる透過 Transport。 */
    private class GateTransport(
        val delegate: LocalRaftTransport,
    ) : RaftTransport {
        val blocked = mutableSetOf<String>()
        val snapshotTargets = mutableListOf<String>()

        override suspend fun requestVote(
            peerId: String,
            request: RequestVoteRequest,
        ): RequestVoteResponse {
            if (peerId in blocked) throw RaftPeerUnavailableException(peerId)
            return delegate.requestVote(peerId, request)
        }

        override suspend fun appendEntries(
            peerId: String,
            request: AppendEntriesRequest,
        ): AppendEntriesResponse {
            if (peerId in blocked) throw RaftPeerUnavailableException(peerId)
            return delegate.appendEntries(peerId, request)
        }

        override suspend fun installSnapshot(
            peerId: String,
            request: InstallSnapshotRequest,
        ): InstallSnapshotResponse {
            snapshotTargets.add(peerId)
            if (peerId in blocked) throw RaftPeerUnavailableException(peerId)
            return delegate.installSnapshot(peerId, request)
        }
    }
}
