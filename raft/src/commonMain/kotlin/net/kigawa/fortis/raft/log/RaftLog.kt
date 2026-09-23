package net.kigawa.fortis.raft.log

interface RaftLog {
    suspend fun lastIndex(): Long

    suspend fun get(
        index: Long,
    ): RaftLogEntry?

    suspend fun append(
        entry: RaftLogEntry,
    )

    suspend fun truncateFrom(
        index: Long,
    )
}