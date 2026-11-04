package net.kigawa.fortis.raft.log

data class RaftLogIndexEntry(
    val offset: Long,
    val length: Int,
)
