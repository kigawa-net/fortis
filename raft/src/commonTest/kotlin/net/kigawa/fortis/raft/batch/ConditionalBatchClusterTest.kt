package net.kigawa.fortis.raft.batch

import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 3 voter の単一 Raft Group で条件付き batch を検証する。
 * 実プロセス・QUIC の 3-voter 受入は未実施（LocalRaftTransport によるインメモリ検証）。
 */
class ConditionalBatchClusterTest {
    private val k1 = byteArrayOf(1)
    private val k2 = byteArrayOf(2)
    private val v1 = byteArrayOf(10)
    private val v2 = byteArrayOf(20)
    private val principal = "tenant-a/alice"

    @Test
    fun committedBatchIsReplicatedAndResubmissionReturnsStoredResult() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        val client = ConditionalBatchClient(leader.runtime)
        val request = request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1)))

        assertIs<RequestResultLookup.ReceiptExpiredOrUnknown>(
            client.lookupResult(principal, "r1", request.requestDigest),
        )
        val first = assertIs<ConditionalBatchResponse.Stored>(client.submit(request))
        assertEquals(BatchOutcome.Applied(2L), first.receipt.outcome)
        leader.runtime.onHeartbeatTimeout()
        for (node in cluster.nodes.values) {
            assertContentEquals(v1, node.machine.get(k1))
            assertEquals(first.receipt, node.machine.receipts.find(principal, "r1"))
        }

        // 同一要求の再送は再提案せず保存結果を返す
        val again = assertIs<ConditionalBatchResponse.Stored>(client.submit(request))
        assertEquals(first.receipt, again.receipt)
        assertEquals(2L, leader.log.lastIndex())
    }

    @Test
    fun digestMismatchAndRevisionCasSurviveLeaderChangeWithoutReexecution() = runTest {
        val cluster = cluster()
        val oldLeader = cluster.nodes.getValue("node-1")
        oldLeader.runtime.onElectionTimeout()
        val request = request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1)))
        ConditionalBatchClient(oldLeader.runtime).submit(request)
        oldLeader.runtime.onHeartbeatTimeout()

        // 旧 Leader を到達不能にして node-2 が Leader になる（応答喪失の後の再送に相当）
        cluster.transport.unavailablePeers.add("node-1")
        val newLeader = cluster.nodes.getValue("node-2")
        newLeader.runtime.onElectionTimeout()
        assertIs<LeaderNode>(newLeader.runtime.currentNode)
        val client = ConditionalBatchClient(newLeader.runtime)

        val resent = assertIs<ConditionalBatchResponse.Stored>(client.submit(request))
        assertEquals(BatchOutcome.Applied(2L), resent.receipt.outcome)
        assertEquals(1, batchEntries(newLeader.log))

        val mismatch = request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v2)))
        assertEquals(ConditionalBatchResponse.DigestMismatch, client.submit(mismatch))

        // 新 Leader 上でも revision による CAS が継続して働く
        val next = request("r2", listOf(KvCondition.RevisionEquals(k1, 2L)), listOf(KvMutation.Put(k1, v2)))
        val applied = assertIs<ConditionalBatchResponse.Stored>(client.submit(next))
        val outcome = assertIs<BatchOutcome.Applied>(applied.receipt.outcome)
        assertTrue(outcome.raftIndex > 2L)
        newLeader.runtime.onHeartbeatTimeout()
        assertContentEquals(v2, newLeader.machine.get(k1))
        assertEquals(2, batchEntries(newLeader.log))
    }

    @Test
    fun rejectedBatchIsRecordedAndReturnedAsStoredResultOnResubmission() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        val client = ConditionalBatchClient(leader.runtime)
        client.submit(request("seed", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))))

        val losing = request("loser", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k2, v2)))
        val rejected = assertIs<ConditionalBatchResponse.Stored>(client.submit(losing))
        assertEquals(BatchOutcome.Rejected(0, observedRevision = 2L), rejected.receipt.outcome)
        leader.runtime.onHeartbeatTimeout()

        for (node in cluster.nodes.values) {
            assertNotNull(node.machine.receipts.find(principal, "loser"))
            assertEquals(null, node.machine.get(k2))
        }
        val again = assertIs<ConditionalBatchResponse.Stored>(client.submit(losing))
        assertEquals(rejected.receipt, again.receipt)
    }

    @Test
    fun unconfirmedOutcomeIsReportedAsUnknownNotAsNotExecuted() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        cluster.transport.unavailablePeers.addAll(listOf("node-2", "node-3"))
        val client = ConditionalBatchClient(leader.runtime, 300.milliseconds)
        val request = request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1)))

        assertEquals(ConditionalBatchResponse.OutcomeUnknown, client.submit(request))
    }

    @Test
    fun restartReplayOfCommittedLogReproducesReceiptsAndState() = runTest {
        val cluster = cluster()
        val leader = cluster.nodes.getValue("node-1")
        leader.runtime.onElectionTimeout()
        val client = ConditionalBatchClient(leader.runtime)
        client.submit(request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))))
        client.submit(request("r2", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v2))))
        client.submit(request("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))))
        leader.runtime.onHeartbeatTimeout()
        val expected = leader.machine.snapshotData()

        // 再起動相当: 永続化された Raft ログと commitIndex から新しい状態機械を再生する
        val restarted = StorageEngineStateMachine(MemoryMvccStorageEngine())
        val committed = leader.volatileState.commitIndex
        RaftApplier(RaftVolatileState(commitIndex = committed), leader.log, restarted).applyCommitted()

        assertContentEquals(expected, restarted.snapshotData())
        assertEquals(2, restarted.receipts.size)
        // 同一要求の再送は再提案されないため、ログ上の batch は2件のまま
        assertEquals(2, batchEntries(leader.log))
    }

    private fun request(
        requestId: String,
        conditions: List<KvCondition>,
        mutations: List<KvMutation>,
    ) = ConditionalBatchRequest(principal, requestId, conditions, mutations)

    // ログに載った条件付き batch の件数（再送で再提案されていないことの確認に使う）
    private suspend fun batchEntries(log: MemoryRaftLog): Int {
        var count = 0
        for (index in 1L..log.lastIndex()) {
            if (log.get(index)?.command is RaftCommand.ConditionalBatch) count++
        }
        return count
    }

    // proposal はテストの仮想時間で進めるため backgroundScope の context を使う（既存テストと同じ方式）
    private suspend fun TestScope.cluster(): Cluster {
        val local = LocalRaftTransport()
        val transport = BlockingTransport(local)
        val nodes = mutableMapOf<String, NodeFixture>()
        for (nodeId in listOf("node-1", "node-2", "node-3")) {
            nodes[nodeId] = nodeFixture(nodeId, transport, backgroundScope.coroutineContext)
        }
        for ((nodeId, fixture) in nodes) {
            local.register(nodeId, fixture.runtime)
        }
        return Cluster(nodes, transport)
    }

    private suspend fun nodeFixture(
        nodeId: String,
        transport: RaftTransport,
        context: CoroutineContext,
    ): NodeFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val machine = StorageEngineStateMachine(MemoryMvccStorageEngine())
        val node = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = setOf("node-1", "node-2", "node-3") - nodeId,
            persistentStateStore = MemoryRaftPersistentStateStore(RaftPersistentState(0)),
            volatileState = volatileState,
            log = log,
            stateMachine = machine,
        ).build()
        return NodeFixture(RaftRuntime(node, transport, context), volatileState, log, machine)
    }

    private data class Cluster(
        val nodes: Map<String, NodeFixture>,
        val transport: BlockingTransport,
    )

    private data class NodeFixture(
        val runtime: RaftRuntime,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val machine: StorageEngineStateMachine,
    )

    private class BlockingTransport(
        private val delegate: RaftTransport,
    ) : RaftTransport {
        val unavailablePeers = mutableSetOf<String>()

        override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse {
            if (peerId in unavailablePeers) throw RaftPeerUnavailableException(peerId)
            return delegate.requestVote(peerId, request)
        }

        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse {
            if (peerId in unavailablePeers) throw RaftPeerUnavailableException(peerId)
            return delegate.appendEntries(peerId, request)
        }
    }
}
