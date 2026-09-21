package net.kigawa.fortis.raft

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

class RaftApplier(
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val stateMachine: RaftStateMachine,
) {
    private val mutex = Mutex()

    suspend fun applyCommitted(): Unit = mutex.withLock {
        while (volatileState.lastApplied < volatileState.commitIndex) {
            val index = volatileState.lastApplied + 1
            val entry = checkNotNull(log.get(index)) {
                "Committed Raft log entry is missing at index $index"
            }

            stateMachine.apply(entry.command)
            volatileState.lastApplied = index
        }
    }
}
