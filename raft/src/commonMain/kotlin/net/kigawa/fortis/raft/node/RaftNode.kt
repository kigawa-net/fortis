package net.kigawa.fortis.raft.node

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.append.*
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RequestVoteHandler
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

abstract class RaftNode(
    var peers: Map<String, RaftPeerProgress>,
    val requestVoteHandler: RequestVoteHandler,
    val appendEntriesHandler: AppendEntriesHandler,
    val appendEntriesResponseHandler: AppendEntriesResponseHandler,
    val appendEntriesFactory: AppendEntriesFactory,
    val commandAppender: CommandAppender,
    val electionStarter: ElectionStarter,
    val requestVoteResponseHandler: RequestVoteResponseHandler,
    val timer: RaftTimer,
) {
    internal val mutex = Mutex()
    private val votesGranted = mutableSetOf<String>()
    var role: RaftRole = RaftRole.FOLLOWER
        private set

    private fun setRole(role: RaftRole) {
        this.role = role
    }

    suspend fun handleRequestVote(request: RequestVoteRequest): RequestVoteResponse = mutex.withLock {
        requestVoteHandler.handle(request, ::setRole).also { response ->
            if (response.voteGranted) {
                timer.reset(RaftTimeoutEvent.Election)
            }
        }
    }

    suspend fun handleAppendEntries(request: AppendEntriesRequest): AppendEntriesResponse = mutex.withLock {
        appendEntriesHandler.handle(request, ::setRole).also { response ->
            if (request.term >= response.term) {
                timer.reset(RaftTimeoutEvent.Election)
            }
        }
    }


    suspend fun appendCommand(command: RaftCommand): RaftLogEntry = mutex.withLock {
        commandAppender.append(command, role, peers.values)
    }

    suspend fun handleAppendEntriesResponse(
        peerId: String, request: AppendEntriesRequest, response: AppendEntriesResponse,
    ): Unit = mutex.withLock {
        peers = appendEntriesResponseHandler.handle(peerId, request, response, role, peers, ::setRole)
    }

    suspend fun startElection(): RequestVoteRequest = mutex.withLock {
        startElectionLocked()
    }

    suspend fun handleRequestVoteResponse(peerId: String, response: RequestVoteResponse): Unit = mutex.withLock {
        val previousRole = role
        peers = requestVoteResponseHandler.handle(peerId, response, peers, role, votesGranted, ::setRole)
        if (previousRole != RaftRole.LEADER && role == RaftRole.LEADER) {
            timer.reset(RaftTimeoutEvent.Heartbeat)
        }
    }

    suspend fun onElectionTimeout(): RequestVoteRequest = mutex.withLock {
        startElectionLocked()
    }



    private suspend fun startElectionLocked(): RequestVoteRequest {
        val request = electionStarter.startElection(
            votesGranted, peers
        ).apply { role = second }
            .apply { peers = third }
            .first
        timer.reset(
            if (role == RaftRole.LEADER) {
                RaftTimeoutEvent.Heartbeat
            } else {
                RaftTimeoutEvent.Election
            }
        )
        return request
    }

}
