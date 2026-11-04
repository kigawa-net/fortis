package net.kigawa.fortis.raft.vote

data class RaftVolatileState(
    var commitIndex: Long = 0,
    var lastApplied: Long = 0,
)