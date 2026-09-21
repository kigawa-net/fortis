package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RequestVoteResponseHandler(
    private val persistentState: RaftPersistentState,
    private val electionStarter: ElectionStarter,
) {
    suspend fun handle(
        peerId: String,
        response: RequestVoteResponse,
        peers: Map<String, RaftPeerProgress>,
        role: RaftRole,
        votesGranted: MutableSet<String>,
        setRole: (RaftRole) -> Unit,
    ) {
        require(peers.containsKey(peerId)) {
            "Unknown peer: $peerId"
        }

        if (response.term > persistentState.currentTerm) {
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            votesGranted.clear()
            setRole(RaftRole.FOLLOWER)
            return
        }

        if (
            role != RaftRole.CANDIDATE ||
            response.term != persistentState.currentTerm ||
            !response.voteGranted
        ) {
            return
        }

        votesGranted.add(peerId)
        if (electionStarter.hasMajority(peers, votesGranted)) {
            electionStarter.becomeLeader(peers, setRole)
        }
    }
}
