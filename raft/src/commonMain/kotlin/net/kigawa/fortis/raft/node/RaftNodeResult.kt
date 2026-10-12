package net.kigawa.fortis.raft.node

data class RaftNodeResult<T>(
    val node: RaftNode,
    val value: T,
)
