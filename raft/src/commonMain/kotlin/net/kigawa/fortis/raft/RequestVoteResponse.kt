package net.kigawa.fortis.raft

data class RequestVoteResponse(
    val term: Long,
    val voteGranted: Boolean,
)