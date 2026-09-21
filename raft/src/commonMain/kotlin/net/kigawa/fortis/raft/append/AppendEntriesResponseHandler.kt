package net.kigawa.fortis.raft.append


import net.kigawa.fortis.raft.*

class AppendEntriesResponseHandler(
    private val persistentState: RaftPersistentState,
    private val commitAdvancer: RaftCommitAdvancer,
    private val applier: RaftApplier,
) {

    suspend fun handle(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
        role: RaftRole,
        peers: Map<String, RaftPeerProgress>,
        setRole: (RaftRole) -> Unit,
    ) {
        if (role != RaftRole.LEADER) {
            return
        }
        val progress =
            requireNotNull(peers[peerId]) {
                "Unknown peer: $peerId"
            }

        val previousTerm =
            persistentState.currentTerm

        handleInternal(
            progress = progress,
            peers = peers.values,
            request = request,
            response = response,
        )

        if (
            persistentState.currentTerm >
            previousTerm
        ) {
            setRole(RaftRole.FOLLOWER)
        }
    }

    private suspend fun handleInternal(
        progress: RaftPeerProgress,
        peers: Collection<RaftPeerProgress>,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ) {
        if (response.term > persistentState.currentTerm) {
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            return
        }

        if (!response.success) {
            progress.nextIndex =
                maxOf(1L, progress.nextIndex - 1)
            return
        }

        val lastSentIndex =
            request.entries.lastOrNull()?.index
                ?: request.prevLogIndex

        progress.matchIndex =
            maxOf(
                progress.matchIndex,
                lastSentIndex,
            )

        progress.nextIndex =
            progress.matchIndex + 1

        commitAdvancer.advance(peers)
        applier.applyCommitted()
    }
}
