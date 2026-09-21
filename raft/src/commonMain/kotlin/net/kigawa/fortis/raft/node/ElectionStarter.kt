package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RequestVoteRequest

data class ElectionStarter(
    val persistentState: RaftPersistentState,
    val nodeId: String,
    val log: RaftLog,
) {

    suspend fun startElection(
        votesGranted: MutableSet<String>,
        peers: Map<String, RaftPeerProgress>,
        setRole: (RaftRole) -> Unit,
    ): RequestVoteRequest {
        check(persistentState.currentTerm < Long.MAX_VALUE) {
            "Raft term is exhausted"
        }

        persistentState.currentTerm++
        persistentState.votedFor = nodeId
        setRole(RaftRole.CANDIDATE)
        votesGranted.clear()
        votesGranted.add(nodeId)

        if (hasMajority(peers, votesGranted)) {
            becomeLeader(peers, setRole)
        }

        val lastLogIndex = log.lastIndex()
        return RequestVoteRequest(
            term = persistentState.currentTerm,
            candidateId = nodeId,
            lastLogIndex = lastLogIndex,
            lastLogTerm = log.get(lastLogIndex)?.term ?: 0L,
        )
    }

    fun hasMajority(peers: Map<String, RaftPeerProgress>, votesGranted: MutableSet<String>): Boolean {
        val clusterSize = peers.size + 1
        return votesGranted.size >= clusterSize / 2 + 1
    }

    suspend fun becomeLeader(peers: Map<String, RaftPeerProgress>, setRole: (RaftRole) -> Unit) {
        setRole(RaftRole.LEADER)
        val nextIndex = log.lastIndex() + 1
        for (progress in peers.values) {
            progress.nextIndex = nextIndex
            progress.matchIndex = 0
        }
    }
}