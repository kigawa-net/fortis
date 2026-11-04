package net.kigawa.fortis.raft.snapshot

/**
 * スナップショットデータを状態機械へ反映する処理。
 *
 * Raft ログ層は [data] を不透明バイト列として扱い、MVCC 層や Range 層への
 * 具体的な復元は実装側に委ねる。何もしない既定実装は [NoOp] を使う。
 */
fun interface RaftSnapshotApplier {
    suspend fun apply(
        metadata: SnapshotMetadata,
        data: ByteArray,
    )

    companion object {
        val NoOp = RaftSnapshotApplier { _, _ -> }
    }
}
