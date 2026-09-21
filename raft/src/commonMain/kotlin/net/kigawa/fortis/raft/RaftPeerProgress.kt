package net.kigawa.fortis.raft

data class RaftPeerProgress(
    val nextIndex: Long,
    var matchIndex: Long = 0,
)