package net.kigawa.fortis.raft

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class MemoryRaftPersistentStateStore(
    initialState: RaftPersistentState = RaftPersistentState(),
) : RaftPersistentStateStore {
    private val mutex = Mutex()
    private var state = initialState.copy()

    override suspend fun load(): RaftPersistentState = mutex.withLock {
        state.copy()
    }

    override suspend fun save(term: Long, votedFor: String?) {
        require(term >= 0) { "Raft term must not be negative" }
        mutex.withLock {
            state = RaftPersistentState(term, votedFor)
        }
    }
}
