package net.kigawa.fortis.raft

data class RequestVoteRequest(
    val term: Long,
    val candidateId: String,
    val lastLogIndex: Long,
    val lastLogTerm: Long,
)