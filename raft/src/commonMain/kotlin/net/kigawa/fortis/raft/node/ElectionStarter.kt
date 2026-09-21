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
    ): Triple<RequestVoteRequest, RaftRole, Map<String, RaftPeerProgress>> {
        check(persistentState.currentTerm < Long.MAX_VALUE) {
            "Raft term is exhausted"
        }

        persistentState.currentTerm++
        persistentState.votedFor = nodeId
        var role = RaftRole.CANDIDATE
        votesGranted.clear()
        votesGranted.add(nodeId)

        if (hasMajority(peers, votesGranted)) {
            role = becomeLeader(peers).second
        }

        val lastLogIndex = log.lastIndex()
        return Triple(
            RequestVoteRequest(
                term = persistentState.currentTerm,
                candidateId = nodeId,
                lastLogIndex = lastLogIndex,
                lastLogTerm = log.get(lastLogIndex)?.term ?: 0L,
            ),
            role,
            peers
        )
    }

    fun hasMajority(peers: Map<String, RaftPeerProgress>, votesGranted: MutableSet<String>): Boolean {
        val clusterSize = peers.size + 1
        return votesGranted.size >= clusterSize / 2 + 1
    }

    suspend fun becomeLeader(
        peers: Map<String, RaftPeerProgress>,
    ): Pair<Map<String, RaftPeerProgress>, RaftRole> {
        val nextIndex = log.lastIndex() + 1
        return Pair(
            peers.mapValues { it.value.copy(nextIndex = nextIndex, matchIndex = 0) },
            RaftRole.LEADER
        )
    }
}