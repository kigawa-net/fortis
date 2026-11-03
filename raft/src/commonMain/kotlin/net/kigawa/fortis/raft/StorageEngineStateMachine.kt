package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.batch.ConditionalBatchApplier
import net.kigawa.fortis.raft.batch.ConditionalBatchCodec
import net.kigawa.fortis.raft.batch.RequestReceiptLedger
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.mvcc.MvccScanResult
import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

class StorageEngineStateMachine(
    private val storage: FortisStorageEngine,
): VersionedRaftStateMachine {
    /** 条件付き batch の受付記録。適用と同一の決定で更新される。 */
    val receipts = RequestReceiptLedger()

    override suspend fun apply(command: RaftCommand) = command.execute(storage)

    override suspend fun applyAt(command: RaftCommand, version: Long) {
        if (command is RaftCommand.ConditionalBatch) {
            ConditionalBatchApplier.apply(command.request, version, versioned(), receipts)
        } else {
            command.executeAt(storage, version)
        }
    }

    /** Local read; call through RaftRuntime.linearizableRead for a quorum-confirmed read. */
    suspend fun get(key: ByteArray): ByteArray? = storage.get(key)

    /** 適用済みの最大 revision（Raft log index）。線形化可能読み取りの中で呼ぶこと。 */
    suspend fun latestRevision(): Long = versioned().latestVersion()

    /** revision 固定の prefix 走査 1 ページ。線形化可能読み取りの中で呼ぶこと。 */
    suspend fun scanPrefixAt(
        prefix: ByteArray,
        readRevision: Long,
        startAfter: ByteArray?,
        limit: Int,
    ): MvccScanResult = versioned().scanPrefixAt(prefix, readRevision, startAfter, limit)

    /**
     * MVCC 履歴と受付記録のスナップショット。呼び出し側は適用が止まった状態
     * （[net.kigawa.fortis.raft.runtime.RaftRuntime.linearizableRead] 内など）で取得すること。
     */
    suspend fun snapshotData(): ByteArray = ConditionalBatchCodec.encodeSnapshot(
        versioned().exportHistory(),
        receipts.entries(),
    )

    /**
     * [snapshotData] の内容を空の状態機械へ復元する。途中失敗時は部分的に復元され得るため、
     * 失敗時は状態機械を破棄すること。
     */
    suspend fun restoreSnapshot(data: ByteArray) {
        val (history, receiptEntries) = ConditionalBatchCodec.decodeSnapshot(data)
        val target = versioned()
        check(target.latestVersion() == 0L && receipts.size == 0) {
            "Snapshot restore target must be empty"
        }
        for (record in history) {
            val value = record.value
            if (value != null) {
                target.putAt(record.key, value, record.version)
            } else {
                target.deleteAt(record.key, record.version)
            }
        }
        receipts.restore(receiptEntries)
    }

    private fun versioned(): VersionedFortisStorageEngine =
        checkNotNull(storage as? VersionedFortisStorageEngine) {
            "Conditional batch requires a versioned storage engine"
        }
}
