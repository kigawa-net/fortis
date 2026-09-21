package net.kigawa.fortis.raft.append

data class AppendEntriesResponse(
    val term: Long,
    val success: Boolean,
)