package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand

data class RaftLogEntry(
    val index: Long,
    val term: Long,
    val payload: RaftLogEntryPayload,
) {
    constructor(index: Long, term: Long, command: RaftCommand) :
        this(index, term, RaftLogEntryPayload.Command(command))

    /** Application command, or null for a Raft internal entry. */
    val command: RaftCommand?
        get() = (payload as? RaftLogEntryPayload.Command)?.command

    init {
        require(index > 0) {
            "Raft log index must be positive"
        }

        require(term >= 0) {
            "Raft term must not be negative"
        }
    }
}
