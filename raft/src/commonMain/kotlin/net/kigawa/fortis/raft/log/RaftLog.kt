package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.snapshot.SnapshotMetadata

interface RaftLog {
    /** 圧縮済みの境界。未圧縮ならnull。 */
    suspend fun snapshotMetadata(): SnapshotMetadata? = null

    /** 境界を含むログの任期を返す。 */
    suspend fun termAt(index: Long): Long? =
        get(index)?.term ?: snapshotMetadata()?.takeIf { it.lastIncludedIndex == index }?.lastIncludedTerm

    /** 適用済み境界まで削除し、不一致の後続ログを破棄する。 */
    suspend fun compactThrough(metadata: SnapshotMetadata, appliedIndex: Long) {
        throw UnsupportedOperationException("Log compaction is not supported")
    }

    suspend fun lastIndex(): Long

    suspend fun get(
        index: Long,
    ): RaftLogEntry?

    suspend fun append(
        entry: RaftLogEntry,
    )

    suspend fun truncateFrom(
        index: Long,
    )
}