package net.kigawa.fortis.raft.snapshot

/**
 * リーダー側の InstallSnapshot 送信判断ヘルパー。
 *
 * フォロワーの nextIndex がリーダーのスナップショット境界以下の場合、
 * 対応するログは既に圧縮済みのため AppendEntries では追随できず、
 * InstallSnapshot を送る必要がある。RaftRuntime への配線は将来課題とし、
 * ここでは判断式のみを提供する。
 */
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
