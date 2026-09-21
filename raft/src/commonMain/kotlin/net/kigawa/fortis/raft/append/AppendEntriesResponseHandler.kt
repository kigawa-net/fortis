package net.kigawa.fortis.raft.append


import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState

class AppendEntriesResponseHandler(
    private val persistentState: RaftPersistentState,
    private val commitAdvancer: RaftCommitAdvancer,
) {
    suspend fun handle(
        progress: RaftPeerProgress,
        peers: Collection<RaftPeerProgress>,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ){
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
    }
}