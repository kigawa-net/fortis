package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.Fs
import net.kigawa.fortis.io.fs.deleteIfExists
import net.kigawa.fortis.raft.FileRaftPersistentStateStore
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.batch.BatchMode
import net.kigawa.fortis.raft.batch.BatchOutcome
import net.kigawa.fortis.raft.batch.KvMutation
import net.kigawa.fortis.raft.log.FileRaftLog
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.MultiRaftRouter
import net.kigawa.fortis.raft.sharding.RangeMetadata
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class KvTransactionCoordinatorTest {
    private val first = byteArrayOf(1)
    private val second = byteArrayOf(2)
    private val remote = byteArrayOf(20)
    private val old = byteArrayOf(40)
    private val updated = byteArrayOf(41)
    private val table = MultiRaftRouter.fromMetadata(
        listOf(
            RangeMetadata(KeyRange("left", null, byteArrayOf(10)), epoch = 2),
            RangeMetadata(KeyRange("right", byteArrayOf(10), null), epoch = 3),
        ),
    )

    @Test
    fun singleRangeWritesUseOneBatchAndOneVersion() = runTest {
        val cluster = cluster()
        val before = cluster.left.log.lastIndex()
        val txn = cluster.coordinator().begin()
        assertEquals(KvTransactionState.IN_FLIGHT, cluster.coordinator().record(txn.id)?.state)
        txn.write(first, old)
        txn.write(second, updated)
        assertContentEquals(old, txn.read(first))
        assertNull(cluster.left.machine.get(first))
        assertTrue(txn.commit())
        assertTrue(txn.commit())

        val index = cluster.left.log.lastIndex()
        assertEquals(before + 1, index)
        val batch = assertIs<RaftCommand.ConditionalBatch>(cluster.left.log.get(index)?.command)
        assertEquals(2, batch.request.mutations.size)
        assertEquals(BatchMode.APPLY, batch.request.mode)
        assertEquals(index, cluster.left.machine.versionOf(first))
        assertEquals(index, cluster.left.machine.versionOf(second))
        assertContentEquals(old, cluster.left.machine.get(first))
        assertContentEquals(updated, cluster.left.machine.get(second))
        assertEquals(KvTransactionRecord(txn.id, KvTransactionState.COMMITTED, true), cluster.coordinator().record(txn.id))

        val deletion = cluster.coordinator().begin()
        deletion.write(first, null)
        assertNull(deletion.read(first))
        assertTrue(deletion.commit())
        assertNull(cluster.left.machine.get(first))
    }

    @Test
    fun readModifyWriteRechecksVersionAtRaftApplyAndRejectsAba() = runTest {
        val cluster = cluster()
        val service = cluster.coordinator()
        val seed = service.begin()
        seed.write(first, old)
        assertTrue(seed.commit())

        val stale = service.begin()
        val observed = checkNotNull(stale.read(first))
        stale.write(first, byteArrayOf((observed.single() + 1).toByte()))
        stale.write(second, updated)
        // Advance the version after the coordinator's read, before the participant batch applies.
        cluster.journal.log.beforeAppend = { entry ->
            if (entry.journalPhase() == JournalPhase.EXECUTING) {
                cluster.left.runtime.propose(RaftCommand.Put(first, updated))
                cluster.left.runtime.propose(RaftCommand.Put(first, old))
            }
        }
        assertFalse(stale.commit())
        val batch = assertIs<RaftCommand.ConditionalBatch>(cluster.left.log.get(cluster.left.log.lastIndex())?.command)
        val receipt = cluster.left.machine.receipts.find(batch.request.principal, stale.id.value)
        val rejected = assertIs<BatchOutcome.Rejected>(receipt?.outcome)
        assertEquals(cluster.left.machine.versionOf(first), rejected.observedRevision)
        assertContentEquals(old, cluster.left.machine.get(first))
        assertNull(cluster.left.machine.get(second))
    }

    @Test
    fun concurrentReadModifyWritesPreserveBothIncrementsAfterRetry() = runTest {
        val cluster = cluster()
        val seed = cluster.coordinator().begin()
        seed.write(first, old)
        seed.write(remote, old)
        assertTrue(seed.commit())

        val a = cluster.coordinator().begin()
        val b = cluster.coordinator().begin()
        suspend fun increment(txn: KvTransactionCoordinator.Transaction, expected: ByteArray) {
            for (key in listOf(first, remote)) {
                val value = checkNotNull(txn.read(key))
                assertContentEquals(expected, value)
                txn.write(key, byteArrayOf((value.single() + 1).toByte()))
            }
        }
        increment(a, old)
        increment(b, old)
        val aResult = async { a.commit() }
        val bResult = async { b.commit() }
        assertEquals(1, listOf(aResult.await(), bResult.await()).count { it })

        val retry = cluster.coordinator().begin()
        increment(retry, updated)
        assertTrue(retry.commit())

        val reader = cluster.coordinator().begin()
        val twice = byteArrayOf((updated.single() + 1).toByte())
        assertContentEquals(twice, reader.read(first))
        assertContentEquals(twice, reader.read(remote))
        assertTrue(reader.commit())
    }

    @Test
    fun readsStayOnOneSnapshotAndStaleReadOnlyCommitFails() = runTest {
        val cluster = cluster()
        val service = cluster.coordinator()
        val seed = service.begin()
        seed.write(first, old)
        seed.write(remote, old)
        assertTrue(seed.commit())

        val reader = service.begin()
        assertContentEquals(old, reader.read(first))
        val writer = cluster.coordinator().begin()
        writer.write(first, updated)
        writer.write(remote, updated)
        assertTrue(writer.commit())
        assertContentEquals(old, reader.read(remote))
        assertContentEquals(old, reader.read(first))
        assertFalse(reader.commit())

        val fresh = service.begin()
        assertContentEquals(updated, fresh.read(first))
        assertContentEquals(updated, fresh.read(remote))
        assertTrue(fresh.commit())
    }

    @Test
    fun readOnlyParticipantCanRejectAndPreviouslyPreparedWritesStayHidden() = runTest {
        val cluster = cluster()
        val service = cluster.coordinator()
        val txn = service.begin()
        assertNull(txn.read(remote))
        txn.write(first, updated)
        cluster.journal.log.beforeAppend = { entry ->
            if (entry.journalPhase() == JournalPhase.EXECUTING) {
                cluster.right.runtime.propose(RaftCommand.Put(remote, old))
            }
        }

        assertFalse(txn.commit())
        assertEquals(KvTransactionRecord(txn.id, KvTransactionState.ABORTED, true), service.record(txn.id))
        assertNull(cluster.left.machine.get(first))
        val prepare = assertIs<RaftCommand.ConditionalBatch>(cluster.left.log.get(cluster.left.log.lastIndex())?.command)
        assertEquals(BatchMode.PREPARE, prepare.request.mode)
        assertIs<BatchOutcome.Applied>(
            cluster.left.machine.receipts.find(prepare.request.principal, prepare.request.requestId)?.outcome,
        )
        assertNull(service.recover())
    }

    @Test
    fun twoCoordinatorsCannotCommitWriteSkew() = runTest {
        val cluster = cluster()
        val a = cluster.coordinator().begin()
        val b = cluster.coordinator().begin()
        assertNull(a.read(remote))
        assertNull(b.read(first))
        a.write(first, updated)
        b.write(remote, updated)
        val firstCommit = async { a.commit() }
        val secondCommit = async { b.commit() }
        assertEquals(1, listOf(firstCommit.await(), secondCommit.await()).count { it })
    }

    @Test
    fun restartAfterPrepareRecoversDurableStateAndCommits() = runTest {
        withRaftFiles { prefix ->
            val original = cluster(prefix)
            val service = original.coordinator()
            val open = service.begin()
            val abandoned = service.begin()
            abandoned.write(second, updated)
            assertEquals(KvTransactionState.ABORTED, abandoned.abort())
            val txn = service.begin()
            txn.write(first, updated)
            txn.write(remote, updated)
            original.journal.log.beforeAppend = { entry ->
                if (entry.journalPhase() == JournalPhase.COMMIT_DECIDED) throw SimulatedCrash()
            }
            assertFailsWith<SimulatedCrash> { txn.commit() }
            assertNull(original.left.machine.get(first))
            assertNull(original.right.machine.get(remote))
            original.stop()

            val restarted = cluster(prefix)
            val recovery = restarted.coordinator()
            assertEquals(KvTransactionRecord(txn.id, KvTransactionState.PREPARED, false), recovery.record(txn.id))
            assertEquals(KvTransactionRecord(open.id, KvTransactionState.IN_FLIGHT, false), recovery.record(open.id))
            assertEquals(KvTransactionState.ABORTED, recovery.abort(open.id))
            assertEquals(KvTransactionRecord(abandoned.id, KvTransactionState.ABORTED, true), recovery.record(abandoned.id))
            assertNull(restarted.left.machine.get(second))
            assertEquals(KvTransactionRecord(txn.id, KvTransactionState.COMMITTED, true), recovery.recover())
            assertContentEquals(updated, restarted.left.machine.get(first))
            assertContentEquals(updated, restarted.right.machine.get(remote))
            assertNull(recovery.recover())
            restarted.stop()
        }
    }

    @Test
    fun restartAfterOnePrepareAbortsWhenTheOtherRangeHasAConflict() = runTest {
        withRaftFiles { prefix ->
            val original = cluster(prefix)
            val service = original.coordinator()
            val txn = service.begin()
            assertNull(txn.read(remote))
            txn.write(first, updated)
            txn.write(remote, updated)
            original.journal.log.beforeAppend = { entry ->
                if (entry.journalPhase() == JournalPhase.EXECUTING) {
                    original.right.runtime.propose(RaftCommand.Put(remote, old))
                }
            }
            original.right.log.beforeAppend = { entry ->
                val command = entry.command as? RaftCommand.ConditionalBatch
                if (command?.request?.mode == BatchMode.PREPARE) throw SimulatedCrash()
            }

            assertFailsWith<SimulatedCrash> { txn.commit() }
            val prepareIndex = original.left.log.lastIndex()
            val prepare = assertIs<RaftCommand.ConditionalBatch>(original.left.log.get(prepareIndex)?.command)
            assertEquals(BatchMode.PREPARE, prepare.request.mode)
            assertIs<BatchOutcome.Applied>(
                original.left.machine.receipts.find(prepare.request.principal, prepare.request.requestId)?.outcome,
            )
            assertNull(original.left.machine.get(first))
            original.stop()

            val restarted = cluster(prefix)
            val recovery = restarted.coordinator()
            assertEquals(KvTransactionRecord(txn.id, KvTransactionState.IN_FLIGHT, false), recovery.record(txn.id))
            val aborted = KvTransactionRecord(txn.id, KvTransactionState.ABORTED, true)
            assertEquals(aborted, recovery.recover())
            assertEquals(aborted, recovery.record(txn.id))
            assertNull(recovery.recover())

            val reader = recovery.begin()
            assertNull(reader.read(first))
            assertContentEquals(old, reader.read(remote))
            assertTrue(reader.commit())
            restarted.stop()
        }
    }

    @Test
    fun restartAfterPartialCommitResumesWithoutReapplyingTheFirstRange() = runTest {
        withRaftFiles { prefix ->
            val original = cluster(prefix)
            val service = original.coordinator()
            val reader = service.begin()
            val txn = service.begin()
            txn.write(first, updated)
            txn.write(remote, updated)
            original.right.log.beforeAppend = { entry ->
                val command = entry.command as? RaftCommand.ConditionalBatch
                if (command?.request?.mode == BatchMode.COMMIT_PREPARED) throw SimulatedCrash()
            }
            assertFailsWith<SimulatedCrash> { txn.commit() }
            assertEquals(KvTransactionRecord(txn.id, KvTransactionState.COMMITTED, false), service.record(txn.id))
            assertNull(reader.read(first))
            assertNull(reader.read(remote))
            assertContentEquals(updated, original.left.machine.get(first))
            assertNull(original.right.machine.get(remote))
            val firstVersion = original.left.machine.versionOf(first)
            original.stop()

            val restarted = cluster(prefix)
            val recovery = restarted.coordinator()
            // begin resolves the durable decision before choosing its snapshot.
            val read = recovery.begin()
            assertContentEquals(updated, read.read(first))
            assertContentEquals(updated, read.read(remote))
            assertTrue(read.commit())
            assertEquals(firstVersion, restarted.left.machine.versionOf(first))
            assertEquals(KvTransactionRecord(txn.id, KvTransactionState.COMMITTED, true), recovery.record(txn.id))
            restarted.stop()
        }
    }

    private fun RaftLogEntry.journalPhase(): JournalPhase? {
        val batch = command as? RaftCommand.ConditionalBatch ?: return null
        val mutation = batch.request.mutations.first() as KvMutation.Put
        return JournalRecord.decode(mutation.value).phase
    }

    @OptIn(ExperimentalUuidApi::class)
    private suspend fun withRaftFiles(test: suspend (String) -> Unit) {
        val prefix = "./fortis-txn-test-${Uuid.random()}"
        try {
            // File operations dispatch to real I/O threads; Raft deadlines use real time here.
            withContext(Dispatchers.Default) { test(prefix) }
        } finally {
            withContext(NonCancellable) {
                for (id in listOf("left", "right", "journal")) {
                    for (suffix in listOf("log", "state", "state.tmp")) {
                        Fs.getPath("$prefix-$id.$suffix").toFile().deleteIfExists()
                    }
                }
            }
        }
    }

    private suspend fun TestScope.cluster(prefix: String? = null): Cluster =
        Cluster(node("left", prefix), node("right", prefix), node("journal", prefix))

    private suspend fun TestScope.node(id: String, prefix: String?): Node {
        val store: RaftPersistentStateStore = if (prefix == null) MemoryRaftPersistentStateStore()
        else FileRaftPersistentStateStore(Fs.getPath("$prefix-$id.state").toFile())
        val log = TestLog(
            if (prefix == null) MemoryRaftLog() else FileRaftLog.open(Fs.getPath("$prefix-$id.log").toFile()),
        )
        val machine = StorageEngineStateMachine(MemoryMvccStorageEngine())
        val node = RaftNodeBuilder(id, emptySet(), store, RaftVolatileState(), log, machine).build()
        val context = if (prefix == null) backgroundScope.coroutineContext
        else backgroundScope.coroutineContext + Dispatchers.Default
        val runtime = RaftRuntime(node, LocalRaftTransport(), context)
        runtime.onElectionTimeout()
        return Node(runtime, log, machine)
    }

    private inner class Cluster(val left: Node, val right: Node, val journal: Node) {
        fun coordinator() = KvTransactionCoordinator(
            table, mapOf("left" to left.runtime, "right" to right.runtime), journal.runtime,
        )

        suspend fun stop() {
            for (node in listOf(left, right, journal)) node.runtime.stop()
        }
    }

    private class TestLog(private val delegate: RaftLog) : RaftLog by delegate {
        var beforeAppend: suspend (RaftLogEntry) -> Unit = {}

        override suspend fun append(entry: RaftLogEntry) {
            beforeAppend(entry)
            delegate.append(entry)
        }
    }

    private data class Node(val runtime: RaftRuntime, val log: TestLog, val machine: StorageEngineStateMachine)

    private class SimulatedCrash : RuntimeException()
}
