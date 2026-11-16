package net.kigawa.fortis.raft.snapshot

/** フォロワーのnextIndexとスナップショット境界から転送要否を判定する。 */
object InstallSnapshotPolicy {
    /**
     * フォロワーへ InstallSnapshot を送るべき場合に真を返す。
     */
    fun shouldSendSnapshot(
        peerNextIndex: Long,
        lastIncludedIndex: Long,
    ): Boolean {
        require(peerNextIndex > 0) {
            "フォロワーの nextIndex は正でなければならない"
        }
        require(lastIncludedIndex > 0) {
            "スナップショットの最終取込インデックスは正でなければならない"
        }
        return peerNextIndex <= lastIncludedIndex
    }

    /** スナップショット適用後にフォロワーが次に求めるログインデックスを返す。 */
    fun nextIndexAfterSnapshot(metadata: SnapshotMetadata): Long {
        check(metadata.lastIncludedIndex < Long.MAX_VALUE) {
            "スナップショットのインデックスが上限に達している"
        }
        return metadata.lastIncludedIndex + 1
    }
}
