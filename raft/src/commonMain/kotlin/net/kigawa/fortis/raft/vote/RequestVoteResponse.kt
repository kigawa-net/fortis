package net.kigawa.fortis.raft.vote

data class RequestVoteResponse(
    val term: Long,
    val voteGranted: Boolean,
)