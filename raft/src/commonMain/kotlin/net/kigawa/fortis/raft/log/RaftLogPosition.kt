package net.kigawa.fortis.raft.log

data class RaftLogPosition(
    val index: Long,
    val term: Long,
)