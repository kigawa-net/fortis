package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand

data class RaftLogEntry(
    val index: Long,
    val term: Long,
    val command: RaftCommand,
) {
    init {
        require(index > 0) {
            "Raft log index must be positive"
        }

        require(term >= 0) {
            "Raft term must not be negative"
        }
    }
}