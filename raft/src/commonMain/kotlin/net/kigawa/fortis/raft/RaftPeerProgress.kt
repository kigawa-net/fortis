package net.kigawa.fortis.raft

data class RaftPeerProgress(
    var nextIndex: Long,
    var matchIndex: Long = 0,
)