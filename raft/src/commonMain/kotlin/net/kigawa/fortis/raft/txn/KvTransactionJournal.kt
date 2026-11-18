package net.kigawa.fortis.raft.txn

import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.batch.BatchMode
import net.kigawa.fortis.raft.batch.BatchOutcome
import net.kigawa.fortis.raft.batch.BinaryReader
import net.kigawa.fortis.raft.batch.BinaryWriter
import net.kigawa.fortis.raft.batch.ConditionalBatchClient
import net.kigawa.fortis.raft.batch.ConditionalBatchCodec
import net.kigawa.fortis.raft.batch.ConditionalBatchRequest
import net.kigawa.fortis.raft.batch.ConditionalBatchResponse
import net.kigawa.fortis.raft.batch.KvCondition
import net.kigawa.fortis.raft.batch.KvMutation
import net.kigawa.fortis.raft.runtime.RaftRuntime

internal class KvTransactionJournal(private val runtime: RaftRuntime) {
    private val client = ConditionalBatchClient(runtime)

    suspend fun gate(): Versioned<TxnId?> = read { gateOf(it) }

    suspend fun record(id: TxnId): Versioned<JournalRecord?> = read { recordOf(it, id) }

    suspend fun active(): Pair<Versioned<TxnId?>, Versioned<JournalRecord?>> = read { machine ->
        val gate = gateOf(machine)
        val id = gate.value
        gate to if (id == null) Versioned(null, 0) else recordOf(machine, id)
    }

    suspend fun update(
        id: TxnId,
        current: Versioned<JournalRecord?>,
        next: JournalRecord,
        gate: Versioned<TxnId?>? = null,
    ): Boolean {
        val key = recordKey(id)
        val conditions = mutableListOf<KvCondition>(KvCondition.RevisionEquals(key, current.revision))
        val mutations = mutableListOf<KvMutation>(KvMutation.Put(key, next.encode()))
        val requestId = if (gate == null) {
            "${id.value}/${current.revision}/${next.phase.name}"
        } else {
            conditions += KvCondition.RevisionEquals(GATE, gate.revision)
            if (next.phase == JournalPhase.EXECUTING) {
                check(gate.value == null)
                mutations += KvMutation.Put(GATE, id.value.encodeToByteArray())
            } else if (next.phase.complete && gate.value != null) {
                check(gate.value == id)
                mutations += KvMutation.Delete(GATE)
            }
            "${id.value}/${current.revision}/${next.phase.name}/${gate.revision}"
        }
        val request = ConditionalBatchRequest(JOURNAL_PRINCIPAL, requestId, conditions, mutations)
        return client.submitFor(id, request)
    }

    private suspend fun gateOf(machine: StorageEngineStateMachine): Versioned<TxnId?> =
        Versioned(machine.get(GATE)?.decodeToString()?.let(::TxnId), machine.versionOf(GATE))

    private suspend fun recordOf(machine: StorageEngineStateMachine, id: TxnId): Versioned<JournalRecord?> {
        val key = recordKey(id)
        return Versioned(machine.get(key)?.let(JournalRecord::decode), machine.versionOf(key))
    }

    private suspend inline fun <T> read(crossinline block: suspend (StorageEngineStateMachine) -> T): T =
        runtime.linearizableRead { block(it as StorageEngineStateMachine) }

    private fun recordKey(id: TxnId) = "record/${id.value}".encodeToByteArray()

    private companion object {
        val GATE = "active".encodeToByteArray()
        const val JOURNAL_PRINCIPAL = "fortis/kv-transaction-journal"
    }
}

internal data class Versioned<out T>(val value: T, val revision: Long)

internal enum class JournalPhase(val tag: Byte, val publicState: KvTransactionState, val complete: Boolean = false) {
    OPEN(1, KvTransactionState.IN_FLIGHT),
    EXECUTING(2, KvTransactionState.IN_FLIGHT),
    PREPARED(3, KvTransactionState.PREPARED),
    COMMIT_DECIDED(4, KvTransactionState.COMMITTED),
    COMMITTED(5, KvTransactionState.COMMITTED, complete = true),
    ABORTED(6, KvTransactionState.ABORTED, complete = true),
}

internal data class RangeBatch(val rangeId: String, val epoch: Long, val request: ConditionalBatchRequest)

internal data class JournalRecord(val phase: JournalPhase, val batches: List<RangeBatch> = emptyList()) {
    fun encode(): ByteArray {
        val writer = BinaryWriter()
        writer.byte(FORMAT_VERSION)
        writer.byte(phase.tag)
        writer.int(batches.size)
        for (batch in batches) {
            writer.string(batch.rangeId)
            writer.long(batch.epoch)
            writer.bytes(ConditionalBatchCodec.encode(batch.request))
        }
        val bytes = writer.toByteArray()
        require(bytes.size <= ConditionalBatchRequest.MAX_VALUE_BYTES) { "Transaction plan exceeds the Raft record limit" }
        return bytes
    }

    companion object {
        private const val FORMAT_VERSION: Byte = 1

        fun decode(bytes: ByteArray): JournalRecord {
            val reader = BinaryReader(bytes)
            require(reader.byte() == FORMAT_VERSION) { "Unsupported transaction record format" }
            val tag = reader.byte()
            val phase = JournalPhase.entries.firstOrNull { it.tag == tag }
                ?: throw IllegalArgumentException("Unknown transaction phase: $tag")
            val count = reader.int()
            require(count in 0..reader.remaining / 16) { "Invalid transaction participant count" }
            val batches = List(count) {
                RangeBatch(reader.string(), reader.long(), ConditionalBatchCodec.decode(reader.bytes()))
            }
            reader.requireEnd()
            return JournalRecord(phase, batches)
        }
    }
}

internal suspend fun ConditionalBatchClient.submitFor(id: TxnId, request: ConditionalBatchRequest): Boolean =
    when (val result = submit(request)) {
        is ConditionalBatchResponse.Stored -> result.receipt.outcome is BatchOutcome.Applied
        ConditionalBatchResponse.DigestMismatch -> error("Conflicting batch for transaction ${id.value}")
        ConditionalBatchResponse.OutcomeUnknown -> throw KvTransactionOutcomeUnknownException(id)
    }

internal fun ConditionalBatchRequest.withMode(requestId: String, mode: BatchMode) =
    ConditionalBatchRequest(principal, requestId, conditions, mutations, mode)
