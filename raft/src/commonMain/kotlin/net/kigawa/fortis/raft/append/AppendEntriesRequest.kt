package net.kigawa.fortis.raft.append

import net.kigawa.fortis.raft.log.RaftLogEntry

data class AppendEntriesRequest(
    val term: Long,
    val leaderId: String,
    val prevLogIndex: Long,
    val prevLogTerm: Long,
    val entries: List<RaftLogEntry>,
    val leaderCommit: Long,
)