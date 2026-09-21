package net.kigawa.fortis.raft.append

import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState

class AppendEntriesResponseHandler(
    private val persistentState: RaftPersistentState,
    private val commitAdvancer: RaftCommitAdvancer,
    private val applier: RaftApplier,
) {
    suspend fun handle(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
        peers: Map<String, RaftPeerProgress>,
    ): Map<String, RaftPeerProgress> {
        val progress = requireNotNull(peers[peerId]) {
            "Unknown peer: $peerId"
        }

        if (response.term > persistentState.currentTerm) {
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            return peers
        }

        val updatedProgress = if (response.success) {
            val lastSentIndex = request.entries.lastOrNull()?.index
                ?: request.prevLogIndex
            val matchIndex = maxOf(progress.matchIndex, lastSentIndex)
            progress.copy(
                nextIndex = matchIndex + 1,
                matchIndex = matchIndex,
            )
        } else {
            progress.copy(nextIndex = maxOf(1L, progress.nextIndex - 1))
        }
        val updatedPeers = peers + (peerId to updatedProgress)

        if (response.success) {
            commitAdvancer.advance(updatedPeers.values)
            applier.applyCommitted()
        }
        return updatedPeers
    }
}
