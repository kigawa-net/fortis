package net.kigawa.fortis.raft

data class RaftPersistentState(
    var currentTerm: Long = 0,
    var votedFor: String? = null,
)