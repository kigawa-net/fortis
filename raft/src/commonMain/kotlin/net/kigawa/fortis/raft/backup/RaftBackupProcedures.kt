package net.kigawa.fortis.raft.backup

import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata

/**
 * backup/restore ドリルの手順。
 *
 * [backup] は [RaftPersistentStateStore] の term/votedFor と
 * 先頭から [upToIndex] までのログエントリを束ねる。[upToIndex] が 0 の場合は
 * 空のエントリ列になる。[metadata] は backup 時点のスナップショット境界であり、
 * ログ層の解釈対象ではなく復元時の参照情報として保持される。
 *
 * [restore] は既存ログと競合しない前提であり、復元先ログは空でなければならない
 * （空でない場合は事前に truncate すること）。永続状態の保存は
 * 既存ストアの単調性制約（term 不減少・同 term の投票不変）に従うため、
 * 新規ストアへの復元を推奨する。復元は非アトミックなドリル用プリミティブであり、
 * 途中失敗時は term/votedFor のみ保存済みになる場合がある。
 */
object RaftBackupProcedures {
    suspend fun backup(
        log: RaftLog,
        store: RaftPersistentStateStore,
        upToIndex: Long,
        metadata: SnapshotMetadata? = null,
    ): RaftBackup {
        require(upToIndex >= 0) { "Raft backup index must not be negative" }
        val lastIndex = log.lastIndex()
        require(upToIndex <= lastIndex) {
            "Raft backup index $upToIndex exceeds last log index $lastIndex"
        }
        val state = store.load()
        val entries = ArrayList<RaftLogEntry>()
        var index = 1L
        while (index <= upToIndex) {
            val entry = log.get(index)
                ?: throw IllegalStateException("Raft log is missing entry at index $index")
            check(entry.index == index) { "Raft log index mismatch at index $index" }
            entries.add(entry)
            index++
        }
        return RaftBackup(state.currentTerm, state.votedFor, entries, metadata)
    }

    suspend fun restore(
        backup: RaftBackup,
        log: RaftLog,
        store: RaftPersistentStateStore,
    ) {
        val existing = log.lastIndex()
        require(existing == 0L) {
            "Raft restore target log must be empty (lastIndex=$existing); truncate first"
        }
        store.save(backup.term, backup.votedFor)
        for (entry in backup.entries) {
            log.append(entry)
        }
    }
}
