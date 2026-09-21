package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand

data class RaftLogEntry(
    val index: Long,
    val term: Long,
    val command: RaftCommand,
)