package net.kigawa.fortis.raft.leader

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesFactory
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.append.AppendEntriesResponseHandler
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.CommandAppender
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState

class LeaderNode(
    nodeId: String,
    peerIds: Set<String>,
    persistentState: RaftPersistentState,
    persistentStateStore: RaftPersistentStateStore,
    volatileState: RaftVolatileState,
    log: RaftLog,
    stateMachine: RaftStateMachine,
    timer: RaftTimer,
    peerProgress: Map<String, RaftPeerProgress>,
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
    private val mutex = Mutex()
    private val appendEntriesFactory = AppendEntriesFactory(
        nodeId,
        persistentState,
        volatileState,
        log,
    )
    private val appendEntriesResponseHandler = AppendEntriesResponseHandler(
        persistentState,
        persistentStateStore,
        commitAdvancer,
        raftApplier,
    )
    private val commandAppender = CommandAppender(
        persistentState,
        log,
        commitAdvancer,
        raftApplier,
    )

    var peerProgress: Map<String, RaftPeerProgress> = peerProgress
        private set

    suspend fun appendCommand(command: RaftCommand): RaftLogEntry =
        mutex.withLock {
            commandAppender.append(command, peerProgress.values)
        }

    suspend fun createAppendEntries(peerId: String): AppendEntriesRequest =
        mutex.withLock {
            appendEntriesFactory.create(peerId, peerProgress)
        }

    suspend fun handleAppendEntriesResponse(
        peerId: String,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    ): RaftNode = mutex.withLock {
        val previousTerm = persistentState.currentTerm
        peerProgress = appendEntriesResponseHandler.handle(
            peerId,
            request,
            response,
            peerProgress,
        )
        if (response.term > previousTerm) follower() else this
    }

    suspend fun onHeartbeatTimeout(): Map<String, AppendEntriesRequest> =
        mutex.withLock {
            val requests = peerIds.associateWith { peerId ->
                appendEntriesFactory.create(peerId, peerProgress)
            }
            timer.reset(RaftTimeoutEvent.Heartbeat)
            requests
        }
}
