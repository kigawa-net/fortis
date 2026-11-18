package net.kigawa.fortis.raft.meta

import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.io.fs.deleteIfExists
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeId
import net.kigawa.fortis.raft.sharding.RangeMetadata
import net.kigawa.fortis.raft.sharding.RangeState
import net.kigawa.fortis.raft.snapshot.FileRaftSnapshotStore
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.transport.InMemoryRaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest

class MetaRaftTest {
    @Test
    fun bootstrapCommitsInitialConfigAndIsRepeatable() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val first = cluster.open("first")
        val other = cluster.open("other")
        assertNull(first.localSnapshot().membership)
        assertFailsWith<IllegalArgumentException> { other.bootstrap() }

        first.bootstrap()
        val leader = assertIs<LeaderNode>(first.runtime.currentNode)
        assertEquals(2L, leader.volatileState.lastApplied)
        assertEquals(leader.volatileState.lastApplied, leader.volatileState.commitIndex)
        assertEquals(ClusterMembership.initial(listOf(ClusterMember("first"))), first.snapshot().membership)

        first.bootstrap()
        assertEquals(2L, leader.log.lastIndex())
        first.registerNode(node)
        assertEquals(node, first.snapshot().nodes["first"])
    }

    @Test
    fun registryChangesReplicateAndRejectStaleHeartbeats() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val first = cluster.open("first")
        val second = cluster.open("second")
        first.bootstrap()
        first.addLearner("second")

        first.registerNode(node)
        assertTrue(first.catchUpLearner("second"))
        assertEquals(node, second.localSnapshot().nodes["first"])

        val updated = node.copy(address = "next:9000", state = RegisteredNodeState.DRAINING, lastHeartbeat = 11)
        first.updateNode(updated)
        assertFailsWith<IllegalArgumentException> { first.updateNode(node) }
        assertTrue(first.catchUpLearner("second"))
        assertEquals(updated, second.localSnapshot().nodes["first"])

        first.removeNode("first")
        first.removeNode("first")
        assertTrue(first.catchUpLearner("second"))
        assertTrue(first.snapshot().nodes.isEmpty())
        assertTrue(second.localSnapshot().nodes.isEmpty())
    }

    @Test
    fun rangeMetadataAndAtomicSplitReplicateWithEpochsAndTombstone() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val first = cluster.open("first")
        val second = cluster.open("second")
        first.bootstrap()
        first.addLearner("second")
        val parent = RangeMetadata(KeyRange("parent", byteArrayOf(1), byteArrayOf(9)), epoch = 7)
        val left = RangeId("left")
        val right = RangeId("right")
        first.putRange(parent)
        assertTrue(first.catchUpLearner("second"))
        val replicated = second.localSnapshot().ranges.getValue(parent.rangeId)
        assertEquals(parent.keyRange, replicated.keyRange)
        assertEquals(7L, replicated.epoch)
        assertEquals(RangeState.ACTIVE, replicated.state)

        assertFailsWith<IllegalArgumentException> {
            first.splitRange(parent.rangeId, 6, byteArrayOf(5), left, right)
        }
        assertFailsWith<IllegalArgumentException> {
            first.splitRange(parent.rangeId, 7, byteArrayOf(9), left, right)
        }
        assertEquals(setOf(parent.rangeId), first.snapshot().ranges.keys)

        first.splitRange(parent.rangeId, 7, byteArrayOf(5), left, right)
        assertTrue(first.catchUpLearner("second"))
        for (replica in listOf(first, second)) {
            val ranges = replica.localSnapshot().ranges
            assertEquals(setOf(parent.rangeId, left, right), ranges.keys)
            assertEquals(RangeState.DELETED, ranges.getValue(parent.rangeId).state)
            assertTrue(ranges.values.all { it.epoch > parent.epoch })
            assertTrue(ranges.getValue(left).keyRange.contains(byteArrayOf(4)))
            assertFalse(ranges.getValue(left).keyRange.contains(byteArrayOf(5)))
            assertContentEquals(byteArrayOf(5), ranges.getValue(right).keyRange.startInclusive)
            assertContentEquals(byteArrayOf(9), ranges.getValue(right).keyRange.endExclusive)
        }
        assertFailsWith<IllegalArgumentException> {
            first.putRange(RangeMetadata(parent.keyRange, 10))
        }
    }

    @Test
    fun learnerInstallsCompactedSnapshotAndReplaysTailBeforePromotion() = runTest {
        val file = FsPath(
            listOf(FsPath.Element.Name("tmp"), FsPath.Element.Name("fortis-meta-${Random.nextLong()}")),
            isAbsolute = true,
        ).toFile()
        try {
            val cluster = Cluster(backgroundScope.coroutineContext)
            val first = cluster.open("first", FileRaftSnapshotStore(file))
            first.bootstrap()
            first.addLearner("second")
            first.registerNode(node)
            val range = RangeMetadata(KeyRange("all", null, null), epoch = 3, state = RangeState.SPLITTING)
            first.putRange(range)
            val boundary = first.saveSnapshot()
            assertNull(first.runtime.currentNode.log.get(boundary.lastIncludedIndex))
            val updated = node.copy(lastHeartbeat = 22)
            first.updateNode(updated)
            val tailIndex = first.runtime.currentNode.log.lastIndex()
            assertEquals(boundary.lastIncludedIndex + 1, tailIndex)
            assertFalse(withContext(Dispatchers.Default) { first.catchUpLearner("second") })
            assertFailsWith<IllegalStateException> { first.promoteLearner("second") }

            val second = cluster.open("second")
            second.runtime.onElectionTimeout()
            assertIs<FollowerNode>(second.runtime.currentNode)
            assertFalse(second.runtime.handleRequestVote(RequestVoteRequest(0, "first", 0, 0)).voteGranted)
            assertTrue(withContext(Dispatchers.Default) { first.catchUpLearner("second") })
            assertEquals(listOf(boundary), cluster.installedSnapshots)
            assertEquals(boundary, second.runtime.currentNode.log.snapshotMetadata())
            val tail = assertIs<RaftLogEntryPayload.Command>(second.runtime.currentNode.log.get(tailIndex)?.payload)
            assertEquals(MetaCommand.UpdateNode(updated), tail.command.toMetaCommand())
            assertEquals(tailIndex, second.runtime.currentNode.volatileState.lastApplied)
            assertEquals(updated, second.localSnapshot().nodes["first"])
            val restoredRange = second.localSnapshot().ranges.getValue(range.rangeId)
            assertEquals(range.keyRange, restoredRange.keyRange)
            assertEquals(3L, restoredRange.epoch)
            assertEquals(RangeState.SPLITTING, restoredRange.state)
            assertEquals(first.localSnapshot().membership, second.localSnapshot().membership)
            assertEquals(ClusterMemberRole.LEARNER, second.localSnapshot().membership?.members?.get("second")?.role)

            first.promoteLearner("second")
            first.runtime.replicate()
            assertEquals(setOf("first", "second"), second.localSnapshot().membership?.voters())
        } finally {
            file.deleteIfExists()
        }
    }

    @Test
    fun promotionRequiresNewQuorumAndCanResumeAcrossLeadershipChange() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val first = cluster.open("first")
        val second = cluster.open("second")
        first.bootstrap()
        first.addLearner("second")
        assertTrue(first.catchUpLearner("second"))

        cluster.blocked.add("second")
        assertFailsWith<TimeoutCancellationException> { first.promoteLearner("second", 100.milliseconds) }
        assertEquals(setOf("first"), first.localSnapshot().membership?.voters())
        assertNull(first.localSnapshot().pendingPromotion)
        assertFailsWith<IllegalStateException> { first.registerNode(node) }

        cluster.blocked.remove("second")
        first.runtime.replicate()
        first.runtime.replicate()
        assertEquals("second", second.localSnapshot().pendingPromotion)
        assertEquals(setOf("first"), second.localSnapshot().membership?.voters())
        second.runtime.onElectionTimeout()
        assertIs<LeaderNode>(second.runtime.currentNode)
        second.runtime.replicate()
        second.promoteLearner("second")
        second.runtime.replicate()
        for (replica in listOf(first, second)) {
            val snapshot = replica.localSnapshot()
            assertEquals(setOf("first", "second"), snapshot.membership?.voters())
            assertEquals(2L, snapshot.membership?.version)
            assertNull(snapshot.pendingPromotion)
        }

        cluster.blocked.add("first")
        assertFailsWith<TimeoutCancellationException> { second.registerNode(node, 100.milliseconds) }
        assertTrue(second.localSnapshot().nodes.isEmpty())
    }

    @Test
    fun seedReplaysBootstrapAndRegistryAfterRestart() = runTest {
        val firstCluster = Cluster(backgroundScope.coroutineContext)
        val log = MemoryRaftLog()
        val termStore = MemoryRaftPersistentStateStore()
        val first = firstCluster.open("first", log = log, termStore = termStore)
        first.bootstrap()
        first.registerNode(node)
        first.runtime.stop()

        val recovered = Cluster(backgroundScope.coroutineContext).open("first", log = log, termStore = termStore)
        recovered.bootstrap()
        assertEquals(first.localSnapshot().membership, recovered.snapshot().membership)
        assertEquals(node, recovered.snapshot().nodes["first"])
    }

    private val node = RegisteredNode("first", "first:9000", RegisteredNodeState.ACTIVE, 10)

    private class Cluster(private val context: CoroutineContext) {
        private val channel = InMemoryRaftRpcChannel()
        private val codec = RaftRpcCodec()
        val blocked = mutableSetOf<String>()
        val installedSnapshots = mutableListOf<SnapshotMetadata>()
        private val transport = RpcRaftTransport(object : RaftRpcChannel {
            override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
                if (peerId in blocked) throw RaftPeerUnavailableException(peerId)
                val message = (codec.decode(frame) as RaftRpcDecodeResult.Success).message
                val response = channel.request(peerId, frame)
                if (message is RaftRpcMessage.InstallSnapshot) installedSnapshots.add(message.request.metadata)
                return response
            }
        })

        suspend fun open(
            nodeId: String,
            snapshotStore: FileRaftSnapshotStore? = null,
            log: MemoryRaftLog = MemoryRaftLog(),
            termStore: MemoryRaftPersistentStateStore = MemoryRaftPersistentStateStore(),
        ): MetaRaft = MetaRaft.open(nodeId, "first", transport, log, termStore, snapshotStore,
            coroutineContext = context).also { channel.register(nodeId, RaftRpcHandler(it.runtime)) }
    }
}
