package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

internal data class RaftCommitQuorum(
    val peers: Collection<RaftPeerProgress>,
    val includesLeader: Boolean = true,
) {
    fun hasMajority(index: Long): Boolean {
        val local = if (includesLeader) 1 else 0
        val replicated = local + peers.count { it.matchIndex >= index }
        return replicated > (local + peers.size) / 2
    }
}

class RaftCommitAdvancer(
    val persistentState: RaftPersistentState,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
) {
    suspend fun advance(peers: Collection<RaftPeerProgress>) = advanceQuorums(listOf(RaftCommitQuorum(peers)))

    internal suspend fun advanceQuorums(quorums: List<RaftCommitQuorum>) {
        val lastIndex = log.lastIndex()

        for (index in lastIndex downTo volatileState.commitIndex + 1) {
            val entry = log.get(index) ?: continue
            if (entry.term != persistentState.currentTerm) continue

            if (quorums.all { it.hasMajority(index) }) {
                volatileState.commitIndex = index
                return
            }
        }
    }

    fun isReplicatedIsMajority(peers: Collection<RaftPeerProgress>, index: Long): Boolean =
        RaftCommitQuorum(peers).hasMajority(index)
}
