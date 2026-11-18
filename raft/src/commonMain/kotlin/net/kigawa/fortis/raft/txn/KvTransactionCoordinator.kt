package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.batch.BatchMode
import net.kigawa.fortis.raft.batch.ConditionalBatchClient
import net.kigawa.fortis.raft.batch.ConditionalBatchRequest
import net.kigawa.fortis.raft.batch.KvCondition
import net.kigawa.fortis.raft.batch.KvMutation
import net.kigawa.fortis.raft.batch.PREPARED_BATCH_SUFFIX
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.sharding.MultiRaftRouter
import net.kigawa.fortis.raft.sharding.RangeRoute
import net.kigawa.fortis.storage.engine.ByteArrayKey
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

enum class KvTransactionState { IN_FLIGHT, PREPARED, COMMITTED, ABORTED }

/** 確定判断と、全参加者への適用が完了したかを表す。 */
data class KvTransactionRecord(val id: TxnId, val state: KvTransactionState, val complete: Boolean)

class KvTransactionOutcomeUnknownException(val transactionId: TxnId) :
    IllegalStateException("Transaction outcome is unconfirmed: ${transactionId.value}")

/**
 * 専用の記録用 Raft グループで確定を直列化する。
 * 管理対象レンジへのクライアント操作はすべてこの API を経由し、確定中はレンジ構成を固定する。
 */
class KvTransactionCoordinator(
    private val router: MultiRaftRouter,
    ranges: Map<String, RaftRuntime>,
    journalRuntime: RaftRuntime,
) {
    private val ranges = ranges.toMap()
    private val clients = this.ranges.mapValues { (_, runtime) -> ConditionalBatchClient(runtime) }
    private val journal = KvTransactionJournal(journalRuntime)

    init {
        require(ranges.keys == router.groups.keys) { "Raft runtimes must cover the routed ranges" }
        require(ranges.values.toSet().size == ranges.size && journalRuntime !in ranges.values) {
            "Each range and the transaction journal need separate Raft runtimes"
        }
    }

    /** 一貫した読取版を確保し、開始を Raft に記録する。 */
    @OptIn(ExperimentalUuidApi::class)
    suspend fun begin(): Transaction {
        val id = TxnId(Uuid.random().toString())
        while (true) {
            val before = journal.gate()
            if (before.value != null) {
                recover()
                continue
            }
            val snapshots = snapshotVersions()
            if (snapshots != snapshotVersions() || before != journal.gate()) continue
            check(journal.update(id, Versioned(null, 0), JournalRecord(JournalPhase.OPEN)))
            return Transaction(id, snapshots)
        }
    }

    /** Raft の記録から状態を照会する。 */
    suspend fun record(id: TxnId): KvTransactionRecord? = journal.record(id).value?.view(id)

    /** 中断した確定処理を再開し、全参加者への適用まで進める。 */
    suspend fun recover(): KvTransactionRecord? {
        var active = journal.active()
        val id = active.first.value ?: return null
        while (active.first.value == id) {
            resolveActive(id, active.first, active.second)
            active = journal.active()
        }
        return record(id)
    }

    /** ID で未着手の取引を破棄する。確定処理中なら決定に従って復旧する。 */
    suspend fun abort(id: TxnId): KvTransactionState? {
        while (true) {
            val current = journal.record(id)
            val phase = current.value?.phase ?: return null
            if (phase.complete) return phase.publicState
            if (phase == JournalPhase.OPEN) {
                journal.update(id, current, JournalRecord(JournalPhase.ABORTED))
            } else {
                recover()
            }
        }
    }

    private suspend fun snapshotVersions(): Map<String, Long> = ranges.mapValues { (_, runtime) ->
        runtime.linearizableRead { (it as StorageEngineStateMachine).latestRevision() }
    }

    inner class Transaction internal constructor(
        val id: TxnId,
        private val snapshots: Map<String, Long>,
    ) {
        private val lock = Mutex()
        private val accesses = mutableMapOf<RangeRoute, Accesses>()
        private var sealing = false

        /** 開始時点の値を読む。自身の書き込みを優先する。 */
        suspend fun read(key: ByteArray): ByteArray? = lock.withLock {
            check(!sealing) { "Transaction has finished accepting operations" }
            val target = checkedKey(key)
            val route = router.routeWithEpoch(target.bytes)
            val access = accesses.getOrPut(route, ::Accesses)
            if (target in access.writes) return access.writes.getValue(target)?.copyOf()
            val value = ranges.getValue(route.rangeId.value).linearizableRead { machine ->
                (machine as StorageEngineStateMachine).getAt(target.bytes, snapshots.getValue(route.rangeId.value))
            }
            access.keys += target
            value?.copyOf()
        }

        /** 書き込みを蓄える。value が null の場合は削除する。 */
        suspend fun write(key: ByteArray, value: ByteArray?) = lock.withLock {
            check(!sealing) { "Transaction has finished accepting operations" }
            require(value == null || value.size <= ConditionalBatchRequest.MAX_VALUE_BYTES)
            val target = checkedKey(key)
            val access = accesses.getOrPut(router.routeWithEpoch(target.bytes), ::Accesses)
            access.keys += target
            access.writes[target] = value?.copyOf()
        }

        /** 版競合があれば false を返す。確定結果が不明な場合は同じ操作を再試行できる。 */
        suspend fun commit(): Boolean = lock.withLock {
            sealing = true
            commitSealed()
        }

        private suspend fun commitSealed(): Boolean {
            while (true) {
                val current = journal.record(id)
                when (checkNotNull(current.value).phase) {
                    JournalPhase.COMMITTED -> return true
                    JournalPhase.ABORTED -> return false
                    JournalPhase.OPEN -> {
                        val gate = journal.gate()
                        if (gate.value != null) {
                            recover()
                            continue
                        }
                        // Build against one idle generation, then acquire it with the same CAS.
                        val batches = plan()
                        if (batches == null || batches.isEmpty()) {
                            val phase = if (batches == null) JournalPhase.ABORTED else JournalPhase.COMMITTED
                            journal.update(id, current, JournalRecord(phase), gate)
                        } else {
                            journal.update(id, current, JournalRecord(JournalPhase.EXECUTING, batches), gate)
                        }
                    }
                    else -> recover()
                }
            }
        }

        /** 確定処理の開始前に破棄する。既に確定を開始していればその結果を返す。 */
        suspend fun abort(): KvTransactionState = lock.withLock {
            sealing = true
            checkNotNull(this@KvTransactionCoordinator.abort(id))
        }

        private suspend fun plan(): List<RangeBatch>? {
            val batches = mutableListOf<RangeBatch>()
            for ((route, access) in accesses.entries.sortedBy { it.key.rangeId.value }) {
                val rangeId = route.rangeId.value
                val snapshot = snapshots.getValue(rangeId)
                if (access.keys.isEmpty()) continue
                val revisions = ranges.getValue(rangeId).linearizableRead { machine ->
                    access.keys.associateWith { (machine as StorageEngineStateMachine).versionOf(it.bytes) }
                }
                if (revisions.values.any { it > snapshot }) return null
                val conditions = revisions.map { (key, revision) -> KvCondition.RevisionEquals(key.bytes, revision) }
                val mutations = access.writes.map { (key, value) ->
                    if (value == null) KvMutation.Delete(key.bytes) else KvMutation.Put(key.bytes, value)
                }
                val mode = if (mutations.isEmpty()) BatchMode.PREPARE else BatchMode.APPLY
                val request = ConditionalBatchRequest(PARTICIPANT_PRINCIPAL, id.value, conditions, mutations, mode)
                batches += RangeBatch(rangeId, route.epoch, request)
            }
            return batches
        }
    }

    // Every helper advances the same durable record. Receipts make proposals safe to replay
    // after a timeout or when a different coordinator was already driving this transaction.
    private suspend fun resolveActive(
        id: TxnId,
        gate: Versioned<TxnId?>,
        current: Versioned<JournalRecord?>,
    ) {
        val record = checkNotNull(current.value)
        record.batches.forEach(::checkRoute)
        val next = when (record.phase) {
            JournalPhase.EXECUTING -> execute(id, record.batches)
            JournalPhase.PREPARED -> JournalPhase.COMMIT_DECIDED
            JournalPhase.COMMIT_DECIDED -> {
                for (batch in record.batches) {
                    if (batch.request.mutations.isEmpty()) continue
                    val commit = batch.request.withMode(id.value, BatchMode.COMMIT_PREPARED)
                    check(clients.getValue(batch.rangeId).submitFor(id, commit)) {
                        "A prepared transaction must accept its commit decision"
                    }
                }
                JournalPhase.COMMITTED
            }
            else -> error("Unexpected active transaction phase: ${record.phase}")
        }
        val nextRecord = if (next.complete) JournalRecord(next) else record.copy(phase = next)
        journal.update(id, current, nextRecord, gate)
    }

    private suspend fun execute(id: TxnId, batches: List<RangeBatch>): JournalPhase {
        if (batches.size == 1) {
            val batch = batches.single()
            val applied = clients.getValue(batch.rangeId).submitFor(id, batch.request)
            return if (applied) JournalPhase.COMMITTED else JournalPhase.ABORTED
        }
        for (batch in batches) {
            val prepare = batch.request.withMode(id.value + PREPARED_BATCH_SUFFIX, BatchMode.PREPARE)
            if (!clients.getValue(batch.rangeId).submitFor(id, prepare)) return JournalPhase.ABORTED
        }
        return JournalPhase.PREPARED
    }

    private fun checkRoute(batch: RangeBatch) {
        for (condition in batch.request.conditions) {
            val current = router.routeWithEpoch(condition.key)
            check(current.rangeId.value == batch.rangeId && current.epoch == batch.epoch) {
                "Transaction range changed before recovery: ${batch.rangeId}"
            }
        }
    }

    private fun JournalRecord.view(id: TxnId) = KvTransactionRecord(id, phase.publicState, phase.complete)

    private fun checkedKey(key: ByteArray): ByteArrayKey {
        require(key.size in 1..ConditionalBatchRequest.MAX_KEY_BYTES)
        return ByteArrayKey(key.copyOf())
    }

    private class Accesses {
        val keys = linkedSetOf<ByteArrayKey>()
        val writes = linkedMapOf<ByteArrayKey, ByteArray?>()
    }

    private companion object {
        const val PARTICIPANT_PRINCIPAL = "fortis/kv-transaction-participant"
    }
}
