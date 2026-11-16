package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MemoryRaftPersistentStateStoreTest {
    @Test
    fun defaultStateHasZeroTermAndNoVote() = runTest {
        val state = MemoryRaftPersistentStateStore().load()

        assertEquals(0L, state.currentTerm)
        assertNull(state.votedFor)
    }

    @Test
    fun saveAtomicallyReplacesTermAndVote() = runTest {
        val store = MemoryRaftPersistentStateStore(
            RaftPersistentState(2, "old-candidate"),
        )

        store.save(3, "new-candidate")

        assertEquals(
            RaftPersistentState(3, "new-candidate"),
            store.load(),
        )
    }

    @Test
    fun loadedStateCannotMutateStoredState() = runTest {
        val store = MemoryRaftPersistentStateStore(
            RaftPersistentState(2, "candidate"),
        )

        store.load().currentTerm = 10

        assertEquals(
            RaftPersistentState(2, "candidate"),
            store.load(),
        )
    }
}
