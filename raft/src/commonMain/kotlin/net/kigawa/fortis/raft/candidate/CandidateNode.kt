package net.kigawa.fortis.raft.candidate

import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeResult
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class CandidateNode(
    nodeId: String,
    peerIds: Set<String>,
    persistentState: RaftPersistentState,
    persistentStateStore: RaftPersistentStateStore,
    volatileState: RaftVolatileState,
    log: RaftLog,
    stateMachine: RaftStateMachine,
    timer: RaftTimer,
    private val votesGranted: MutableSet<String>,
) : RaftNode(
    nodeId,
    peerIds,
    persistentState,
    persistentStateStore,
    volatileState,
    log,
    stateMachine,
    timer,
) {
    suspend fun createRequestVote(): RequestVoteRequest {
        val lastLogIndex = log.lastIndex()
        return RequestVoteRequest(
            term = persistentState.currentTerm,
            candidateId = nodeId,
            lastLogIndex = lastLogIndex,
            lastLogTerm = log.get(lastLogIndex)?.term ?: 0L,
        )
    }

    suspend fun onElectionTimeout(): RaftNodeResult<RequestVoteRequest> {
        check(persistentState.currentTerm < Long.MAX_VALUE) {
            "Raft term is exhausted"
        }
        val nextTerm = persistentState.currentTerm + 1
        persistentStateStore.save(nextTerm, nodeId)
        persistentState.currentTerm = nextTerm
        persistentState.votedFor = nodeId
        votesGranted.clear()
        votesGranted.add(nodeId)
        timer.reset(RaftTimeoutEvent.Election)
        return RaftNodeResult(this, createRequestVote())
    }

    suspend fun handleRequestVoteResponse(
        peerId: String,
        response: RequestVoteResponse,
    ): RaftNode {
        require(peerId in peerIds) { "Unknown peer: $peerId" }

        if (response.term > persistentState.currentTerm) {
            persistentStateStore.save(response.term, null)
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            votesGranted.clear()
            return follower()
        }
        if (
            response.term != persistentState.currentTerm ||
            !response.voteGranted
        ) {
            return this
        }

        votesGranted.add(peerId)
        return if (hasMajority()) {
            becomeLeader().also {
                timer.reset(RaftTimeoutEvent.Heartbeat)
            }
        } else {
            this
        }
    }

    internal fun hasMajority(): Boolean =
        votesGranted.size >= (peerIds.size + 1) / 2 + 1

    internal suspend fun becomeLeader(): LeaderNode {
        val nextIndex = log.lastIndex() + 1
        return LeaderNode(
            nodeId,
            peerIds,
            persistentState,
            persistentStateStore,
            volatileState,
            log,
            stateMachine,
            timer,
            peerIds.associateWith { RaftPeerProgress(nextIndex = nextIndex) },
        )
    }
}
