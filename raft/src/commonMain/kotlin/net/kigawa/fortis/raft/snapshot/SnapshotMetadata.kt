package net.kigawa.fortis.raft.snapshot

/**
 * Raft スナップショットのメタデータ。
 *
 * [lastIncludedIndex] までのログエントリがスナップショットに取り込まれ、
 * 圧縮対象になることを表す。[lastIncludedTerm] はその境界エントリのタームである。
 * MVCC 層のスナップショットとは別物であり、Raft ログ層の圧縮範囲のみを示す。
 */
data class SnapshotMetadata(
    val lastIncludedIndex: Long,
    val lastIncludedTerm: Long,
) {
    init {
        require(lastIncludedIndex > 0) {
            "スナップショットの最終取込インデックスは正でなければならない"
        }
        require(lastIncludedTerm >= 0) {
            "スナップショットの最終取込タームは負であってはならない"
        }
    }
}
