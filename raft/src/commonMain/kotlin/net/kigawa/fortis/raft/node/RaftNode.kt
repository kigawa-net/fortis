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

class RaftNode(
    val peers: MutableMap<String, RaftPeerProgress>,
    val requestVoteHandler: RequestVoteHandler,
    val appendEntriesHandler: AppendEntriesHandler,
    val appendEntriesResponseHandler: AppendEntriesResponseHandler,
    val appendEntriesFactory: AppendEntriesFactory,
    val commandAppender: CommandAppender,
    val electionStarter: ElectionStarter,
    val requestVoteResponseHandler: RequestVoteResponseHandler,
) {
    private val mutex = Mutex()
    private val votesGranted = mutableSetOf<String>()
    var role: RaftRole = RaftRole.FOLLOWER
        private set

    private fun setRole(role: RaftRole) {
        this.role = role
    }

    suspend fun handleRequestVote(request: RequestVoteRequest): RequestVoteResponse = mutex.withLock {
        requestVoteHandler.handle(request, ::setRole)
    }

    suspend fun handleAppendEntries(request: AppendEntriesRequest): AppendEntriesResponse = mutex.withLock {
        appendEntriesHandler.handle(request, ::setRole)
    }

    suspend fun createAppendEntries(peerId: String): AppendEntriesRequest = mutex.withLock {
        appendEntriesFactory.create(peerId, role, peers)
    }

    suspend fun appendCommand(command: RaftCommand): RaftLogEntry = mutex.withLock {
        commandAppender.append(command, role, peers.values)
    }

    suspend fun handleAppendEntriesResponse(
        peerId: String, request: AppendEntriesRequest, response: AppendEntriesResponse,
    ): Unit = mutex.withLock {
        appendEntriesResponseHandler.handle(peerId, request, response, role, peers, ::setRole)
    }

    suspend fun startElection(): RequestVoteRequest = mutex.withLock {
        electionStarter.startElection(votesGranted, peers, ::setRole)
    }

    suspend fun handleRequestVoteResponse(peerId: String, response: RequestVoteResponse): Unit = mutex.withLock {
        requestVoteResponseHandler.handle(peerId, response, peers, role, votesGranted, ::setRole)
    }

}
