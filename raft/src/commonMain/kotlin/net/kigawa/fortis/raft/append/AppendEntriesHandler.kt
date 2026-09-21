package net.kigawa.fortis.raft.append

import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

class AppendEntriesHandler(
    private val persistentState: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val stateMachine: RaftStateMachine,
) {
    suspend fun handle(
        request: AppendEntriesRequest,
    ): AppendEntriesResponse {
        TODO()
    }
}