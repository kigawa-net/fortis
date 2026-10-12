package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand

sealed interface RaftLogEntryPayload {
    data class Command(val command: RaftCommand) : RaftLogEntryPayload
    data object NoOp : RaftLogEntryPayload
}
