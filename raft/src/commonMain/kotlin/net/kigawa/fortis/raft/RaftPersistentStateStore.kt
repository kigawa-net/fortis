package net.kigawa.fortis.raft

interface RaftPersistentStateStore {
    suspend fun load(): RaftPersistentState

    suspend fun save(
        term: Long,
        votedFor: String?,
    )
}
