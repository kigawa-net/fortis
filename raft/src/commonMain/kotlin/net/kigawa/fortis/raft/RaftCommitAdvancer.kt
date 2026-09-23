package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

class RaftCommitAdvancer(
    val persistentState: RaftPersistentState,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
) {
    suspend fun advance(peers: Collection<RaftPeerProgress>) {
        val lastIndex = log.lastIndex()

        for (index in lastIndex downTo volatileState.commitIndex + 1) {
            val entry = log.get(index) ?: continue
            if (entry.term != persistentState.currentTerm) continue

            if (isReplicatedIsMajority(peers, index)) {
                volatileState.commitIndex = index
                return
            }
        }
    }

    fun isReplicatedIsMajority(peers: Collection<RaftPeerProgress>, index: Long): Boolean {
        val replicated = 1 + peers.count {
            it.matchIndex >= index
        }
        val clusterSize = peers.size + 1
        val majority = clusterSize / 2 + 1
        return replicated >= majority
    }
}