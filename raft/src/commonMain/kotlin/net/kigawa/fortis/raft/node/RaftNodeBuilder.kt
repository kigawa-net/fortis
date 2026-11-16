package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

data class RaftNodeBuilder(
    val nodeId: String,
    val peerIds: Set<String>,
    val persistentStateStore: RaftPersistentStateStore,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
    val stateMachine: RaftStateMachine,
    val timer: RaftTimer = RaftTimer.None,
) {
    suspend fun build(): FollowerNode = FollowerNode(
        nodeId,
        peerIds,
        persistentStateStore.load(),
        persistentStateStore,
        volatileState,
        log,
        stateMachine,
        timer,
    )
}
