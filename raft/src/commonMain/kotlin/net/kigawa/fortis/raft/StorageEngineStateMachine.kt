package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.batch.ConditionalBatchApplier
import net.kigawa.fortis.raft.batch.ConditionalBatchCodec
import net.kigawa.fortis.raft.batch.MvccSnapshotMetadata
import net.kigawa.fortis.raft.batch.RequestReceiptLedger
import net.kigawa.fortis.raft.batch.StorageSnapshot
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
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

    internal suspend fun gcSafePoint(): Long = memoryMvcc().gcSafePoint()

    internal suspend fun compactBelowSafePoint(safePoint: Long) {
        (storage as? MemoryMvccStorageEngine)?.compactBelowSafePoint(safePoint)
    }

    /** Local read; call through RaftRuntime.linearizableRead for a quorum-confirmed read. */
    suspend fun get(key: ByteArray): ByteArray? = storage.get(key)

    internal suspend fun getAt(key: ByteArray, revision: Long): ByteArray? = versioned().getAt(key, revision)

    internal suspend fun versionOf(key: ByteArray): Long = versioned().versionOf(key)

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
     * 確定履歴と受付記録を保存する。メモリ MVCC の v2 には圧縮境界と発行済み版も含む。
     * 呼び出し側は適用が止まった状態
     * （[net.kigawa.fortis.raft.runtime.RaftRuntime.linearizableRead] 内など）で取得すること。
     */
    suspend fun snapshotData(): ByteArray {
        val checkpoint = (storage as? MemoryMvccStorageEngine)?.checkpoint()
        return if (checkpoint != null) {
            ConditionalBatchCodec.encodeSnapshot(
                checkpoint.history, receipts.entries(),
                MvccSnapshotMetadata(checkpoint.compactedFloor, checkpoint.issuedTimestamp),
            )
        } else {
            ConditionalBatchCodec.encodeSnapshot(versioned().exportHistory(), receipts.entries())
        }
    }

    /**
     * [snapshotData] の内容を空の状態機械へ復元する。途中失敗時は部分的に復元され得るため、
     * 失敗時は状態機械を破棄すること。
     */
    suspend fun restoreSnapshot(data: ByteArray) {
        val snapshot = ConditionalBatchCodec.decodeSnapshot(data)
        val target = versioned()
        check(target.latestVersion() == 0L && receipts.size == 0) {
            "Snapshot restore target must be empty"
        }
        if (target is MemoryMvccStorageEngine) {
            restoreMvcc(target, snapshot)
            return
        }
        val issued = snapshot.history.maxOfOrNull { it.version } ?: 0L
        require(snapshot.mvcc == null || snapshot.mvcc == MvccSnapshotMetadata(0, issued)) {
            "MVCC snapshot metadata requires MemoryMvccStorageEngine"
        }
        for (record in snapshot.history) {
            val value = record.value
            if (value != null) {
                target.putAt(record.key, value, record.version)
            } else {
                target.deleteAt(record.key, record.version)
            }
        }
        receipts.restore(snapshot.receipts)
    }

    /**
     * Raft の InstallSnapshot でメモリ MVCC の状態を置き換える。
     * 適用と直列化して呼ぶこと。ローカルの pin と未確定予約は失効する。
     */
    suspend fun installSnapshot(data: ByteArray) {
        restoreMvcc(memoryMvcc(), ConditionalBatchCodec.decodeSnapshot(data))
    }

    private suspend fun restoreMvcc(target: MemoryMvccStorageEngine, snapshot: StorageSnapshot) {
        val issued = snapshot.mvcc?.issuedTimestamp ?: snapshot.history.maxOfOrNull { it.version } ?: 0L
        target.installCommittedSnapshot(snapshot.history, snapshot.mvcc?.floor ?: 0L, issued)
        receipts.replace(snapshot.receipts)
    }

    private fun memoryMvcc(): MemoryMvccStorageEngine =
        checkNotNull(storage as? MemoryMvccStorageEngine) { "MemoryMvccStorageEngine required" }

    private fun versioned(): VersionedFortisStorageEngine =
        checkNotNull(storage as? VersionedFortisStorageEngine) {
            "Conditional batch requires a versioned storage engine"
        }
}
