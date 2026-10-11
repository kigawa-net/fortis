package net.kigawa.fortis.raft.backup

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata

/**
 * Raft 永続状態の backup を表す不変の値。
 *
 * [term]/[votedFor] は永続状態、[entries] は先頭から連続したログエントリ、
 * [metadata] は backup 時点のスナップショット境界（無い場合は null）である。
 * [entries] とコマンド内バイト列は防御コピーされる。
 */
class RaftBackup(
    val term: Long,
    val votedFor: String?,
    entries: List<RaftLogEntry>,
    val metadata: SnapshotMetadata?,
) {
    val entries: List<RaftLogEntry> = entries.map { it.copyPayload() }

    init {
        require(term >= 0) { "Raft term must not be negative" }
        entries.forEachIndexed { position, entry ->
            require(entry.index == position + 1L) {
                "Raft backup entries must be contiguous from index 1"
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false
        other as RaftBackup
        return term == other.term &&
            votedFor == other.votedFor &&
            entries == other.entries &&
            metadata == other.metadata
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + (votedFor?.hashCode() ?: 0)
        result = 31 * result + entries.hashCode()
        result = 31 * result + (metadata?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String =
        "RaftBackup(term=$term, votedFor=$votedFor, entries=${entries.size}, metadata=$metadata)"

    private fun RaftLogEntry.copyPayload(): RaftLogEntry {
        val copied = when (val payload = payload) {
            is RaftLogEntryPayload.Command -> when (val command = payload.command) {
                is RaftCommand.Put -> RaftLogEntryPayload.Command(
                    RaftCommand.Put(command.key.copyOf(), command.value.copyOf()),
                )

                is RaftCommand.Delete -> RaftLogEntryPayload.Command(
                    RaftCommand.Delete(command.key.copyOf()),
                )
            }

            RaftLogEntryPayload.NoOp -> payload
        }
        return copy(index = index, term = term, payload = copied)
    }
}
