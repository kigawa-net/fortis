package net.kigawa.fortis.raft.snapshot

import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

/** 永続スナップショットとログ境界を照合し、後続のcommit確認に備える。 */
object RaftSnapshotRecovery {
    suspend fun restore(
        store: FileRaftSnapshotStore,
        log: RaftLog,
        volatileState: RaftVolatileState,
        applier: RaftSnapshotApplier,
    ): SnapshotMetadata? {
        val saved = store.load()
        val base = log.snapshotMetadata()
        if (saved == null) {
            if (base != null) throw RaftSnapshotCorruptionException("圧縮済みログのスナップショットがない")
            return null
        }
        val (metadata, data) = saved
        if (base != null && (base.lastIncludedIndex > metadata.lastIncludedIndex ||
                (base.lastIncludedIndex == metadata.lastIncludedIndex && base != metadata))) {
            throw RaftSnapshotCorruptionException("スナップショットとログ境界が不整合")
        }
        applier.apply(metadata, data)
        volatileState.commitIndex = metadata.lastIncludedIndex
        volatileState.lastApplied = metadata.lastIncludedIndex
        log.compactThrough(metadata, volatileState.lastApplied)
        return metadata
    }
}
