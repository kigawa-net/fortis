package net.kigawa.fortis.raft.batch

import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

/** 条件付き batch の確定結果。どちらも Raft log 順序で一度だけ決まる。 */
sealed interface BatchOutcome {
    /** 全条件が成立し、全変更を [raftIndex] の版で適用した。 */
    data class Applied(val raftIndex: Long) : BatchOutcome

    /** [failedCondition] 番目（conditions の添字）が不成立。変更は一切適用していない。 */
    data class Rejected(val failedCondition: Int, val observedRevision: Long) : BatchOutcome
}

/**
 * 受付記録。(principal, requestId) で一意であり、適用時に [requestDigest] と結果を同時に保存する。
 */
class RequestReceipt(
    val principal: String,
    val requestId: String,
    requestDigest: ByteArray,
    val outcome: BatchOutcome,
) {
    val requestDigest: ByteArray = requestDigest.copyOf()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RequestReceipt) return false
        return principal == other.principal &&
            requestId == other.requestId &&
            requestDigest.contentEquals(other.requestDigest) &&
            outcome == other.outcome
    }

    override fun hashCode(): Int {
        var result = principal.hashCode()
        result = 31 * result + requestId.hashCode()
        result = 31 * result + requestDigest.contentHashCode()
        result = 31 * result + outcome.hashCode()
        return result
    }

    override fun toString(): String =
        "RequestReceipt(principal=$principal, requestId=$requestId, outcome=$outcome)"
}

/**
 * 状態機械が保持する受付記録の台帳。
 *
 * 記録は退役（GC）されない。この slice では受付記録の欠落を「未実行」と解釈してはならず、
 * 呼び出し側は欠落を [RequestResultLookup.ReceiptExpiredOrUnknown] として扱う。
 */
class RequestReceiptLedger {
    private val receipts = mutableMapOf<ReceiptKey, RequestReceipt>()

    val size: Int
        get() = receipts.size

    fun find(principal: String, requestId: String): RequestReceipt? =
        receipts[ReceiptKey(principal, requestId)]

    internal fun record(receipt: RequestReceipt) {
        val key = ReceiptKey(receipt.principal, receipt.requestId)
        check(key !in receipts) { "Receipt already recorded for ${receipt.principal}/${receipt.requestId}" }
        receipts[key] = receipt
    }

    /** 決定的な順序（principal, requestId）の全記録。スナップショット用。 */
    fun entries(): List<RequestReceipt> =
        receipts.values.sortedWith(compareBy({ it.principal }, { it.requestId }))

    /** 空の台帳にのみ復元する。 */
    internal fun restore(entries: List<RequestReceipt>) {
        check(receipts.isEmpty()) { "Receipt ledger restore target must be empty" }
        for (entry in entries) {
            record(entry)
        }
    }

    private data class ReceiptKey(val principal: String, val requestId: String)
}

/** 適用処理の結果。Replayed/DigestMismatch では台帳を変更しない。 */
internal sealed interface ApplyResult {
    data class Recorded(val receipt: RequestReceipt) : ApplyResult
    data class Replayed(val receipt: RequestReceipt) : ApplyResult
    data class DigestMismatch(val existing: RequestReceipt) : ApplyResult
}

/**
 * Raft log の1エントリ分の条件付き batch を決定的に適用する。
 *
 * 受付記録の確認 → 全条件の評価 → 全変更 → 受付記録の保存、の順で行い、
 * 呼び出しは状態機械の適用直列化の内側で行うこと。条件評価は適用時点の確定状態に対して行う。
 */
internal object ConditionalBatchApplier {
    suspend fun apply(
        request: ConditionalBatchRequest,
        raftIndex: Long,
        storage: VersionedFortisStorageEngine,
        ledger: RequestReceiptLedger,
    ): ApplyResult {
        val existing = ledger.find(request.principal, request.requestId)
        if (existing != null) {
            return if (existing.requestDigest.contentEquals(request.requestDigest)) {
                ApplyResult.Replayed(existing)
            } else {
                ApplyResult.DigestMismatch(existing)
            }
        }
        val outcome = failedCondition(request, storage)?.let { (index, observed) ->
            BatchOutcome.Rejected(index, observed)
        } ?: run {
            for (mutation in request.mutations) {
                when (mutation) {
                    is KvMutation.Put -> storage.putAt(mutation.key, mutation.value, raftIndex)
                    is KvMutation.Delete -> storage.deleteAt(mutation.key, raftIndex)
                }
            }
            BatchOutcome.Applied(raftIndex)
        }
        val receipt = RequestReceipt(request.principal, request.requestId, request.requestDigest, outcome)
        ledger.record(receipt)
        return ApplyResult.Recorded(receipt)
    }

    private suspend fun failedCondition(
        request: ConditionalBatchRequest,
        storage: VersionedFortisStorageEngine,
    ): Pair<Int, Long>? {
        for ((index, condition) in request.conditions.withIndex()) {
            val holds = when (condition) {
                is KvCondition.Absent -> storage.get(condition.key) == null
                is KvCondition.RevisionEquals -> storage.versionOf(condition.key) == condition.revision
            }
            if (!holds) return index to storage.versionOf(condition.key)
        }
        return null
    }
}
