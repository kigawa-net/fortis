package net.kigawa.fortis.raft

data class RaftPeerProgress(
    val nextIndex: Long,
    val matchIndex: Long = 0,
)