package net.kigawa.fortis.raft.leader

import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.append.AppendEntriesFactory
import net.kigawa.fortis.raft.append.AppendEntriesHandler
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponseHandler
import net.kigawa.fortis.raft.node.*
import net.kigawa.fortis.raft.vote.RequestVoteHandler

class LeaderNode(
    peers: Map<String, RaftPeerProgress>, requestVoteHandler: RequestVoteHandler,
    appendEntriesHandler: AppendEntriesHandler,
    appendEntriesResponseHandler: AppendEntriesResponseHandler,
    appendEntriesFactory: AppendEntriesFactory, commandAppender: CommandAppender,
    electionStarter: ElectionStarter, requestVoteResponseHandler: RequestVoteResponseHandler,
    timer: RaftTimer,
): RaftNode(
    peers,
    requestVoteHandler, appendEntriesHandler, appendEntriesResponseHandler, appendEntriesFactory,
    commandAppender, electionStarter, requestVoteResponseHandler, timer
) {
    suspend fun createAppendEntries(peerId: String): AppendEntriesRequest = mutex.withLock {
        appendEntriesFactory.create(peerId, peers)
    }

    suspend fun onHeartbeatTimeout(): Map<String, AppendEntriesRequest> = mutex.withLock {
        val requests = mutableMapOf<String, AppendEntriesRequest>()
        for (peerId in peers.keys) {
            requests[peerId] = appendEntriesFactory.create(peerId, peers)
        }
        timer.reset(RaftTimeoutEvent.Heartbeat)
        requests
    }
}