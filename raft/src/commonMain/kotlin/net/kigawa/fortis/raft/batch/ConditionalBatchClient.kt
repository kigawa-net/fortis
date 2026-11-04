package net.kigawa.fortis.raft.batch

import kotlinx.coroutines.TimeoutCancellationException
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.runtime.RaftProposalLeadershipLostException
import net.kigawa.fortis.raft.runtime.RaftProposalNotLeaderException
import net.kigawa.fortis.raft.runtime.RaftProposalStoppedException
import net.kigawa.fortis.raft.runtime.RaftReadLeadershipLostException
import net.kigawa.fortis.raft.runtime.RaftReadNotLeaderException
import net.kigawa.fortis.raft.runtime.RaftReadStoppedException
import net.kigawa.fortis.raft.runtime.RaftRuntime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** 受付記録の照会結果。照会は実行しない。 */
sealed interface RequestResultLookup {
    data class Found(val receipt: RequestReceipt) : RequestResultLookup

    /** 同じ requestId で別の内容の要求が記録されている。保存結果は返さない。 */
    data object DigestMismatch : RequestResultLookup

    /**
     * 受付記録が無い。未実行の証明ではない（未適用・退役・未知を区別しない）。
     * 呼び出し側はこの状態を「未実行」とみなしてはならず、同じ要求を再送して照合すること。
     */
    data object ReceiptExpiredOrUnknown : RequestResultLookup
}

/** [ConditionalBatchClient.submit] の応答。 */
sealed interface ConditionalBatchResponse {
    /** 確定結果（APPLIED/REJECTED）。再送時も同じ結果が返る。 */
    data class Stored(val receipt: RequestReceipt) : ConditionalBatchResponse

    data object DigestMismatch : ConditionalBatchResponse

    /**
     * 確定結果を確認できない（タイムアウト・Leader 変化・Quorum 喪失等）。
     * 異なる requestId で再実行せず、同じ principal/requestId/内容で再送すること。
     */
    data object OutcomeUnknown : ConditionalBatchResponse
}

/**
 * 単一 Raft Group の条件付き batch クライアント（Result Lookup を含む）。
 *
 * 状態機械は [StorageEngineStateMachine] を前提とする。読み取りは [RaftRuntime.linearizableRead] を使う。
 */
class ConditionalBatchClient(
    private val runtime: RaftRuntime,
    private val proposalTimeout: Duration = 5.seconds,
) {
    /** principal・requestId・正規形で受付記録を線形化可能に照会する。実行はしない。 */
    suspend fun lookupResult(
        principal: String,
        requestId: String,
        requestDigest: ByteArray,
    ): RequestResultLookup = runtime.linearizableRead { machine ->
        lookupIn(machine, principal, requestId, requestDigest)
    }

    /**
     * 要求を提出する。受付記録が既にあればそれを返し（再実行しない）、無ければ Raft へ提案して
     * 適用後に結果を照合する。確定結果が確認できない場合は [ConditionalBatchResponse.OutcomeUnknown]。
     */
    suspend fun submit(request: ConditionalBatchRequest): ConditionalBatchResponse = try {
        when (val known = lookupResult(request.principal, request.requestId, request.requestDigest)) {
            is RequestResultLookup.Found -> ConditionalBatchResponse.Stored(known.receipt)
            RequestResultLookup.DigestMismatch -> ConditionalBatchResponse.DigestMismatch
            RequestResultLookup.ReceiptExpiredOrUnknown -> {
                runtime.propose(RaftCommand.ConditionalBatch(request), proposalTimeout)
                toResponse(lookupResult(request.principal, request.requestId, request.requestDigest))
            }
        }
    } catch (cause: Exception) {
        if (cause.isUnconfirmedOutcome()) ConditionalBatchResponse.OutcomeUnknown else throw cause
    }

    private fun toResponse(lookup: RequestResultLookup): ConditionalBatchResponse = when (lookup) {
        is RequestResultLookup.Found -> ConditionalBatchResponse.Stored(lookup.receipt)
        RequestResultLookup.DigestMismatch -> ConditionalBatchResponse.DigestMismatch
        RequestResultLookup.ReceiptExpiredOrUnknown -> ConditionalBatchResponse.OutcomeUnknown
    }

    private fun Exception.isUnconfirmedOutcome(): Boolean =
        this is TimeoutCancellationException ||
            this is RaftProposalNotLeaderException ||
            this is RaftProposalLeadershipLostException ||
            this is RaftProposalStoppedException ||
            this is RaftReadNotLeaderException ||
            this is RaftReadLeadershipLostException ||
            this is RaftReadStoppedException

    private companion object {
        fun lookupIn(
            machine: RaftStateMachine,
            principal: String,
            requestId: String,
            requestDigest: ByteArray,
        ): RequestResultLookup {
            val receipt = checkNotNull(machine as? StorageEngineStateMachine) {
                "Conditional batch lookup requires a StorageEngineStateMachine"
            }.receipts.find(principal, requestId) ?: return RequestResultLookup.ReceiptExpiredOrUnknown
            return if (receipt.requestDigest.contentEquals(requestDigest)) {
                RequestResultLookup.Found(receipt)
            } else {
                RequestResultLookup.DigestMismatch
            }
        }
    }
}
