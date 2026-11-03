package net.kigawa.fortis.raft.scan

import kotlinx.coroutines.TimeoutCancellationException
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.runtime.RaftReadLeadershipLostException
import net.kigawa.fortis.raft.runtime.RaftReadNotLeaderException
import net.kigawa.fortis.raft.runtime.RaftReadStoppedException
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.storage.engine.mvcc.MvccScanResult
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * 単一 Raft Group 内の revision 固定 prefix scan（MVP）。
 *
 * - 初回（cursor・readRevision とも無し）は線形化可能読み取りの中で適用済み最大 revision を確定し、
 *   同じ読み取りの中で 1 ページ目を返す。
 * - 2 ページ目以降は cursor の revision で読む。適用は後続 revision にしか書き込まないため結果は変わらない。
 * - 版が compact で失われていれば [ScanResult.RevisionCompacted]（部分結果は返さない）。
 * - 読み取りを確定できない場合は [ScanResult.Unavailable]、未対応の状態機械は [ScanResult.Unsupported]。
 *
 * 認可（namespace/tenant）・Snapshot lease・Cursor の TTL は未実装。
 */
class RevisionScanClient(
    private val runtime: RaftRuntime,
    private val readTimeout: Duration = 5.seconds,
) {
    suspend fun scanAtRevision(request: ScanRequest): ScanResult {
        val decoded = try {
            request.cursor?.let { OpaqueCursorCodec.decode(it, request.prefix) }
        } catch (cause: InvalidScanCursorException) {
            return ScanResult.InvalidCursor(cause.message ?: "invalid cursor")
        }
        if (decoded != null && request.readRevision != null && request.readRevision != decoded.revision) {
            return ScanResult.InvalidCursor(
                "cursor revision ${decoded.revision} differs from readRevision ${request.readRevision}",
            )
        }
        return try {
            runtime.linearizableRead(readTimeout) { machine -> pageAt(machine, request, decoded) }
        } catch (cause: Exception) {
            cause.unavailableReason()?.let { ScanResult.Unavailable(it) } ?: throw cause
        }
    }

    private suspend fun pageAt(
        machine: RaftStateMachine,
        request: ScanRequest,
        decoded: DecodedCursor?,
    ): ScanResult {
        val state = machine as? StorageEngineStateMachine
            ?: return ScanResult.Unsupported("revision scan requires a StorageEngineStateMachine")
        val latest = state.latestRevision()
        val revision = decoded?.revision ?: request.readRevision ?: latest
        if (revision > latest) return ScanResult.RevisionUnavailable(revision, latest)
        return when (
            val page = state.scanPrefixAt(request.prefix, revision, decoded?.lastKey, request.pageSize)
        ) {
            is MvccScanResult.Compacted -> ScanResult.RevisionCompacted(page.floor)
            is MvccScanResult.Page -> {
                val entries = page.entries.map { ScanEntry(it.key, it.value, it.version) }
                val next = if (page.hasMore) {
                    OpaqueCursorCodec.encode(request.prefix, revision, entries.last().key)
                } else {
                    null
                }
                ScanResult.Page(entries, revision, next)
            }
        }
    }

    private fun Exception.unavailableReason(): String? = when (this) {
        is TimeoutCancellationException -> "TIMEOUT"
        is RaftReadNotLeaderException -> "NOT_LEADER"
        is RaftReadLeadershipLostException -> "LEADERSHIP_LOST"
        is RaftReadStoppedException -> "STOPPED"
        else -> null
    }
}
