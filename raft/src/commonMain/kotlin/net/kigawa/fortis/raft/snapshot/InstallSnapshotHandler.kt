package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState

/**
 * 単一メッセージの InstallSnapshot RPC 受信処理。
 *
 * 受信 term が現 term 以上なら選挙タイマーをリセットする。term が進む場合は
 * 永続状態を保存して追随する。スナップショットが現ログより新しい場合のみ
 * ログを切り詰めてスナップショットを反映し、古いスナップショットは無視して
 * 現 term で応答する。
 *
 * 暫定仕様として、境界エントリが欠落またはターム不一致の場合はローカルログ全体を
 * 破棄する（[RaftLog.truncateFrom] のみで前方圧縮ができないため）。
 * 境界エントリのタームが一致する場合は後続エントリを保持し、境界以前の物理的な
 * 前方圧縮（base-index 対応）は将来課題とする。スナップショット自体の永続化と
 * [RaftTimer] を除く Runtime への配線も将来課題である。
 */
class InstallSnapshotHandler(
    private val persistentState: RaftPersistentState,
    private val persistentStateStore: RaftPersistentStateStore,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val timer: RaftTimer,
    private val snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
    initialLastIncludedIndex: Long = 0,
) {
    private val mutex = Mutex()
    private var lastIncludedIndex = initialLastIncludedIndex
        .also { require(it >= 0) { "適用済みスナップショットのインデックスは負であってはならない" } }

    suspend fun handle(
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse = mutex.withLock {
        if (request.term < persistentState.currentTerm) {
            return@withLock InstallSnapshotResponse(persistentState.currentTerm)
        }

        if (request.term > persistentState.currentTerm) {
            persistentStateStore.save(request.term, null)
            persistentState.currentTerm = request.term
            persistentState.votedFor = null
        }

        timer.reset(RaftTimeoutEvent.Election)

        if (request.metadata.lastIncludedIndex <= lastIncludedIndex) {
            return@withLock InstallSnapshotResponse(persistentState.currentTerm)
        }

        installSnapshot(request.metadata, request.data)
        InstallSnapshotResponse(persistentState.currentTerm)
    }

    private suspend fun installSnapshot(
        metadata: SnapshotMetadata,
        data: ByteArray,
    ) {
        val boundary = log.get(metadata.lastIncludedIndex)
        if (boundary?.term != metadata.lastIncludedTerm) {
            if (log.lastIndex() > 0) {
                log.truncateFrom(1)
            }
        }
        snapshotApplier.apply(metadata, data)
        volatileState.commitIndex = maxOf(volatileState.commitIndex, metadata.lastIncludedIndex)
        volatileState.lastApplied = maxOf(volatileState.lastApplied, metadata.lastIncludedIndex)
        lastIncludedIndex = metadata.lastIncludedIndex
    }
}
