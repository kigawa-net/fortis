package net.kigawa.fortis.raft.vote

data class RequestVoteRequest(
    val term: Long,
    val candidateId: String,
    val lastLogIndex: Long,
    val lastLogTerm: Long,
)