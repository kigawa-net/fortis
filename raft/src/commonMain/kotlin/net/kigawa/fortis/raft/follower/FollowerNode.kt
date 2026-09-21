package net.kigawa.fortis.raft.follower

import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftNodeResult
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest

class FollowerNode(
    nodeId: String,
    peerIds: Set<String>,
    persistentState: RaftPersistentState,
    volatileState: RaftVolatileState,
    log: RaftLog,
    stateMachine: RaftStateMachine,
    timer: RaftTimer,
) : RaftNode(
    nodeId,
    peerIds,
    persistentState,
    volatileState,
    log,
    stateMachine,
    timer,
) {
    suspend fun onElectionTimeout(): RaftNodeResult<RequestVoteRequest> {
        check(persistentState.currentTerm < Long.MAX_VALUE) {
            "Raft term is exhausted"
        }
        persistentState.currentTerm++
        persistentState.votedFor = nodeId

        val candidate = CandidateNode(
            nodeId,
            peerIds,
            persistentState,
            volatileState,
            log,
            stateMachine,
            timer,
            mutableSetOf(nodeId),
        )
        val request = candidate.createRequestVote()
        val nextNode = if (candidate.hasMajority()) {
            candidate.becomeLeader()
        } else {
            candidate
        }
        timer.reset(
            if (nextNode is LeaderNode) {
                RaftTimeoutEvent.Heartbeat
            } else {
                RaftTimeoutEvent.Election
            },
        )
        return RaftNodeResult(nextNode, request)
    }
}
