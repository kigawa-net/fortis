package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.snapshot.FileRaftSnapshotStore
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.snapshot.RaftSnapshotRecovery

data class RaftNodeBuilder(
    val nodeId: String,
    val peerIds: Set<String>,
    val persistentStateStore: RaftPersistentStateStore,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
    val stateMachine: RaftStateMachine,
    val timer: RaftTimer = RaftTimer.None,
) {
    suspend fun build(
        snapshotStore: FileRaftSnapshotStore? = null,
        snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
    ): FollowerNode {
        if (snapshotStore != null) {
            RaftSnapshotRecovery.restore(snapshotStore, log, volatileState, snapshotApplier)
        } else {
            check(log.snapshotMetadata() == null) { "Compacted log requires snapshot recovery" }
        }
        val state = persistentStateStore.load()
        check(state.currentTerm >= (log.snapshotMetadata()?.lastIncludedTerm ?: 0)) {
            "Persistent term is older than snapshot"
        }
        return FollowerNode(
            nodeId,
            peerIds,
            state,
            persistentStateStore,
            volatileState,
            log,
            stateMachine,
            timer,
        )
    }
}
