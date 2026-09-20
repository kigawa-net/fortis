package net.kigawa.fortis.raft

data class RaftLogEntry(
    val index: Long,
    val term: Long,
    val command: RaftCommand,
)