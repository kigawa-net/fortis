package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState

class RaftCommitAdvancer(
    private val persistentState: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
) {
    suspend fun advance(
        peers: Collection<RaftPeerProgress>,
    ) {
        val lastIndex = log.lastIndex()

        for (
            index in lastIndex downTo
                volatileState.commitIndex + 1
        ) {
            val entry =
                log.get(index)
                    ?: continue

            if (
                entry.term !=
                persistentState.currentTerm
            ) {
                continue
            }

            val replicated =
                1 + peers.count {
                    it.matchIndex >= index
                }

            val clusterSize =
                peers.size + 1

            val majority =
                clusterSize / 2 + 1

            if (replicated >= majority) {
                volatileState.commitIndex = index
                return
            }
        }
    }
}