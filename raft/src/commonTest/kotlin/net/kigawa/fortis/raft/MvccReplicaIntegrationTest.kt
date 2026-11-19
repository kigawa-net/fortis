package net.kigawa.fortis.raft

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.Fs
import net.kigawa.fortis.io.fs.deleteIfExists
import net.kigawa.fortis.raft.batch.BatchMode
import net.kigawa.fortis.raft.batch.BatchOutcome
import net.kigawa.fortis.raft.batch.ConditionalBatchRequest
import net.kigawa.fortis.raft.batch.KvCondition
import net.kigawa.fortis.raft.batch.KvMutation
import net.kigawa.fortis.raft.batch.PREPARED_BATCH_SUFFIX
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.FileRaftLog
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.snapshot.FileRaftSnapshotStore
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.transport.InMemoryRaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class MvccReplicaIntegrationTest {
    private val key = byteArrayOf(1)
    private val otherKey = byteArrayOf(2)
    private val first = byteArrayOf(10)
    private val second = byteArrayOf(20)

    @Test
    fun putDeleteAndGarbageCollectionConvergeAcrossLeaderChange() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val a = cluster.open("a")
        val b = cluster.open("b")
        val c = cluster.open("c")

        a.elect()
        val initial = a.propose(RaftCommand.Put(key, first))
        val updated = a.propose(RaftCommand.Put(key, second))
        val deleted = a.propose(RaftCommand.Delete(otherKey))
        val pin = c.storage.retain(initial)
        val gc = a.collectGarbage()
        assertEquals(deleted + 1, gc.garbageCollectionSafePoint)
        assertNull(gc.command)
        assertEquals(gc, gc.copy())
        assertEquals(gc.index, c.runtime.currentNode.volatileState.lastApplied)
        assertEquals(0L, c.storage.checkpoint().compactedFloor)
        assertContentEquals(first, c.storage.getAt(key, initial))
        c.storage.release(pin)

        a.collectGarbage()
        assertEquals(listOf(updated, deleted), a.storage.exportHistory().map { it.version })
        for (peer in listOf(b, c)) assertSameMvcc(a, peer, deleted)

        cluster.setAvailable("a", false)
        b.elect()
        val inserted = b.propose(RaftCommand.Put(otherKey, first))
        val tombstone = b.propose(RaftCommand.Delete(key))
        b.collectGarbage()
        cluster.setAvailable("a", true)
        b.runtime.onHeartbeatTimeout()

        assertIs<FollowerNode>(a.runtime.currentNode)
        assertEquals(listOf(inserted, tombstone), b.storage.exportHistory().map { it.version })
        for (peer in listOf(a, b, c)) {
            assertSameMvcc(b, peer, tombstone)
            assertNull(peer.machine.get(key))
            assertContentEquals(first, peer.machine.get(otherKey))
        }
    }

    @Test
    fun prepareIsAppliedButInvisibleOnEveryReplicaUntilCommit() = runTest {
        val cluster = Cluster(backgroundScope.coroutineContext)
        val leader = cluster.open("a")
        val replicas = listOf(leader, cluster.open("b"), cluster.open("c"))
        leader.elect()

        val conditions = listOf(KvCondition.Absent(key))
        val mutations = listOf(KvMutation.Put(key, first))
        val prepare = ConditionalBatchRequest("client", "txn$PREPARED_BATCH_SUFFIX", conditions, mutations, BatchMode.PREPARE)
        val prepared = leader.propose(RaftCommand.ConditionalBatch(prepare))
        for (replica in replicas) {
            assertEquals(BatchOutcome.Applied(prepared), replica.machine.receipts.find("client", prepare.requestId)?.outcome)
            assertNull(replica.machine.get(key))
            assertNull(replica.machine.getAt(key, prepared))
            assertTrue(replica.storage.exportHistory().isEmpty())
        }

        val commit = ConditionalBatchRequest("client", "txn", conditions, mutations, BatchMode.COMMIT_PREPARED)
        val committed = leader.propose(RaftCommand.ConditionalBatch(commit))
        for (replica in replicas) {
            assertEquals(BatchOutcome.Applied(committed), replica.machine.receipts.find("client", commit.requestId)?.outcome)
            assertContentEquals(first, replica.machine.get(key))
            assertNull(replica.machine.getAt(key, prepared))
            assertContentEquals(first, replica.machine.getAt(key, committed))
        }
    }

    @OptIn(ExperimentalUuidApi::class)
    @Test
    fun compactedSnapshotSurvivesRestartAndReplacesLaggingFollowerState() = runTest {
        val prefix = "./fortis-mvcc-test-${Uuid.random()}"
        try {
            // ファイル I/O と提案の期限を同じ実時間で進める。
            withContext(Dispatchers.Default) {
                val cluster = Cluster(backgroundScope.coroutineContext + Dispatchers.Default, prefix)
                val leader = cluster.open("a")
                cluster.open("b")
                val lagging = cluster.open("c")
                leader.elect()
                val initial = leader.propose(RaftCommand.Put(key, first))
                assertContentEquals(first, lagging.machine.get(key))
                cluster.setAvailable("c", false)

                val update = ConditionalBatchRequest(
                    "client", "update", listOf(KvCondition.RevisionEquals(key, initial)), listOf(KvMutation.Put(key, second)),
                )
                val updated = leader.propose(RaftCommand.ConditionalBatch(update))
                val floor = leader.propose(RaftCommand.Delete(key))
                val gc = leader.collectGarbage()
                val diskLog = FileRaftLog.open(Fs.getPath("$prefix-a.log").toFile())
                assertEquals(gc, diskLog.get(gc.index))
                val reserved = leader.storage.beginWrite(otherKey, first)
                assertEquals(gc.index, reserved.version)
                val metadata = leader.runtime.saveMvccSnapshot()
                assertEquals(gc.index, metadata.lastIncludedIndex)
                val saved = assertNotNull(cluster.snapshotStore("a").load())
                assertEquals(metadata, saved.first)
                assertEquals(2.toByte(), saved.second.first())
                assertNull(leader.runtime.currentNode.log.get(gc.index))
                assertNull(lagging.runtime.currentNode.log.snapshotMetadata())
                leader.runtime.stop()

                val restarted = cluster.open("a")
                assertEquals(metadata, restarted.runtime.currentNode.log.snapshotMetadata())
                assertEquals(gc.index, restarted.runtime.currentNode.volatileState.lastApplied)
                assertRestored(leader, restarted, floor, initial, reserved.version)
                assertNull(lagging.machine.receipts.find("client", update.requestId))

                restarted.elect()
                // 旧 follower のログにない境界を使うため、通常の AppendEntries では追いつけない。
                cluster.setAvailable("c", true)
                restarted.runtime.onHeartbeatTimeout()
                assertEquals(metadata, lagging.runtime.currentNode.log.snapshotMetadata())
                val installed = assertNotNull(cluster.snapshotStore("c").load())
                assertEquals(metadata, installed.first)
                assertContentEquals(saved.second, installed.second)
                assertRestored(leader, lagging, floor, initial, reserved.version)
                for (replica in listOf(restarted, lagging)) {
                    assertEquals(BatchOutcome.Applied(updated), replica.machine.receipts.find("client", update.requestId)?.outcome)
                }
                assertEquals(reserved.version + 1, restarted.storage.nextTimestamp())

                restarted.propose(RaftCommand.Put(key, first))
                assertSameMvcc(restarted, lagging, floor)
                assertContentEquals(first, lagging.machine.get(key))
            }
        } finally {
            withContext(NonCancellable) {
                for (id in nodeIds) {
                    for (suffix in listOf("log", "state", "snapshot", "log.snapshot-tmp", "state.tmp", "snapshot.snapshot-tmp")) {
                        Fs.getPath("$prefix-$id.$suffix").toFile().deleteIfExists()
                    }
                }
            }
        }
    }

    private suspend fun assertRestored(source: Replica, restored: Replica, floor: Long, old: Long, issued: Long) {
        assertSameMvcc(source, restored, floor)
        assertEquals(issued, restored.storage.checkpoint().issuedTimestamp)
        assertFailsWith<IllegalArgumentException> { restored.machine.getAt(key, old) }
        assertNull(restored.machine.getAt(key, floor))
        assertNull(restored.machine.get(key))
        assertNull(restored.machine.get(otherKey))
        assertTrue(restored.storage.checkpoint().uncommittedVersions.isEmpty())
    }

    private suspend fun assertSameMvcc(expected: Replica, actual: Replica, floor: Long) {
        assertEquals(floor, expected.storage.checkpoint().compactedFloor)
        assertEquals(floor, actual.storage.checkpoint().compactedFloor)
        val expectedHistory = expected.storage.exportHistory()
        val actualHistory = actual.storage.exportHistory()
        assertEquals(expectedHistory.map { it.version }, actualHistory.map { it.version })
        for ((left, right) in expectedHistory.zip(actualHistory)) {
            assertContentEquals(left.key, right.key)
            assertContentEquals(left.value, right.value)
        }
    }

    private class Cluster(private val context: CoroutineContext, private val prefix: String? = null) {
        private val network = Mutex()
        private var channel = InMemoryRaftRpcChannel()
        private val handlers = mutableMapOf<String, RaftRpcHandler>()
        private val unavailable = mutableSetOf<String>()
        private val transport = RpcRaftTransport(object : RaftRpcChannel {
            override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
                val target = network.withLock {
                    if (peerId in unavailable) throw RaftPeerUnavailableException(peerId)
                    channel
                }
                return target.request(peerId, frame)
            }
        })

        suspend fun setAvailable(id: String, available: Boolean) = network.withLock {
            if (available) unavailable.remove(id) else unavailable.add(id)
        }

        fun snapshotStore(id: String) = FileRaftSnapshotStore(Fs.getPath("$prefix-$id.snapshot").toFile())

        suspend fun open(id: String): Replica {
            val storage = MemoryMvccStorageEngine()
            val machine = StorageEngineStateMachine(storage)
            val snapshots = prefix?.let { snapshotStore(id) }
            val log = if (prefix == null) MemoryRaftLog() else FileRaftLog.open(Fs.getPath("$prefix-$id.log").toFile())
            val state = if (prefix == null) MemoryRaftPersistentStateStore()
            else FileRaftPersistentStateStore(Fs.getPath("$prefix-$id.state").toFile())
            val node = RaftNodeBuilder(id, nodeIds - id, state, RaftVolatileState(), log, machine)
                .build(snapshots, RaftSnapshotApplier { _, data -> machine.restoreSnapshot(data) })
            val runtime = RaftRuntime(
                node, transport, snapshots, proposalCoroutineContext = context,
                snapshotApplier = RaftSnapshotApplier { _, data -> machine.installSnapshot(data) },
            )
            network.withLock {
                handlers[id] = RaftRpcHandler(runtime)
                channel = InMemoryRaftRpcChannel().also { next ->
                    handlers.forEach { (peer, handler) -> next.register(peer, handler) }
                }
            }
            return Replica(runtime, machine, storage)
        }
    }

    private class Replica(
        val runtime: RaftRuntime,
        val machine: StorageEngineStateMachine,
        val storage: MemoryMvccStorageEngine,
    ) {
        suspend fun elect() {
            runtime.onElectionTimeout()
            assertIs<LeaderNode>(runtime.currentNode)
            runtime.onHeartbeatTimeout()
        }

        suspend fun propose(command: RaftCommand): Long {
            val entry = runtime.propose(command)
            runtime.onHeartbeatTimeout()
            return entry.index
        }

        suspend fun collectGarbage() = runtime.proposeCollectGarbage().also { runtime.onHeartbeatTimeout() }
    }

    private companion object {
        val nodeIds = setOf("a", "b", "c")
    }
}
