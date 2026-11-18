package net.kigawa.fortis.raft.append

import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftCommitQuorum
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore

class AppendEntriesResponseHandler(
    private val persistentState: RaftPersistentState,
    private val persistentStateStore: RaftPersistentStateStore,
    private val commitAdvancer: RaftCommitAdvancer,
    private val applier: RaftApplier,
) {
    suspend fun handle(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
        peers: Map<String, RaftPeerProgress>,
    ): Map<String, RaftPeerProgress> = handle(peerId, request, response, peers) { listOf(RaftCommitQuorum(it.values)) }

    internal suspend fun handle(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
        peers: Map<String, RaftPeerProgress>,
        quorumProgress: suspend (Map<String, RaftPeerProgress>) -> List<RaftCommitQuorum>,
    ): Map<String, RaftPeerProgress> {
        val progress = requireNotNull(peers[peerId]) {
            "Unknown peer: $peerId"
        }

        if (response.term > persistentState.currentTerm) {
            persistentStateStore.save(response.term, null)
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
            commitAdvancer.advanceQuorums(quorumProgress(updatedPeers))
            applier.applyCommitted()
        }
        return updatedPeers
    }
}
