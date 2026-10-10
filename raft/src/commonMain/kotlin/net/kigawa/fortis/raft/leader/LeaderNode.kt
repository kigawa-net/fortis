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
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.node.CommandAppender
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.snapshot.InstallSnapshotPolicy
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
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

    /**
     * リーダーが把握する最新のスナップショット境界。未設定時は従来通り
     * AppendEntries のみで複製する。設定はログ圧縮側の責務であり、
     * スナップショット自体の永続化とチャンク分割転送は将来課題。
     */
    var snapshotMetadata: SnapshotMetadata? = null
        private set

    private var leadershipNoOp: RaftLogEntry? = null

    /** Current-term commit barrier only; linearizable reads still need quorum confirmation. */
    val isReady: Boolean
        get() = leadershipNoOp?.let {
            it.term == persistentState.currentTerm && volatileState.commitIndex >= it.index
        } ?: false

    internal suspend fun initializeLeadership() = mutex.withLock {
        check(leadershipNoOp == null) { "Leadership is already initialized" }
        leadershipNoOp = commandAppender.append(RaftLogEntryPayload.NoOp, peerProgress.values)
    }

    suspend fun appendCommand(command: RaftCommand): RaftLogEntry =
        mutex.withLock {
            commandAppender.append(command, peerProgress.values)
        }

    suspend fun createAppendEntries(peerId: String): AppendEntriesRequest =
        mutex.withLock {
            appendEntriesFactory.create(peerId, peerProgress)
        }

    /** スナップショット境界を設定する。null で未設定に戻し従来動作にする。 */
    suspend fun updateSnapshotMetadata(metadata: SnapshotMetadata?) = mutex.withLock {
        snapshotMetadata = metadata
    }

    /**
     * フォロワーへ InstallSnapshot を送るべき場合に真を返す。
     * 未設定時や nextIndex が境界より進んでいる場合は偽。
     */
    suspend fun needsSnapshot(peerId: String): Boolean = mutex.withLock {
        snapshotForLocked(peerId) != null
    }

    /**
     * スナップショットが必要な場合の境界を返す。不要なら null。
     * 判定と取得を同一ロックで行い、複製判断のずれを防ぐ。
     */
    suspend fun snapshotFor(peerId: String): SnapshotMetadata? = mutex.withLock {
        snapshotForLocked(peerId)
    }

    private fun snapshotForLocked(peerId: String): SnapshotMetadata? {
        val metadata = snapshotMetadata ?: return null
        val progress = requireNotNull(peerProgress[peerId]) {
            "Unknown peer: $peerId"
        }
        return if (InstallSnapshotPolicy.shouldSendSnapshot(progress.nextIndex, metadata.lastIncludedIndex)) {
            metadata
        } else {
            null
        }
    }

    /**
     * InstallSnapshot 要求を生成する。[data] は呼び出し側が
     * [net.kigawa.fortis.raft.snapshot.RaftSnapshotProvider] 等で供給し、
     * ログ層は内容を解釈しない。
     */
    suspend fun createInstallSnapshot(
        peerId: String,
        data: ByteArray,
    ): InstallSnapshotRequest = mutex.withLock {
        requireNotNull(peerProgress[peerId]) {
            "Unknown peer: $peerId"
        }
        val metadata = checkNotNull(snapshotMetadata) {
            "Snapshot metadata is not set"
        }
        InstallSnapshotRequest(
            term = persistentState.currentTerm,
            leaderId = nodeId,
            metadata = metadata,
            data = data,
        )
    }

    /**
     * InstallSnapshot 応答を反映する。成功時は peer の nextIndex を
     * 境界の次に進め、matchIndex を境界まで引き上げる。
     * 高ターム応答時は降格してフォロワーに戻る。
     */
    suspend fun handleInstallSnapshotResponse(
        peerId: String,
        request: InstallSnapshotRequest,
        response: InstallSnapshotResponse,
    ): RaftNode = mutex.withLock {
        val previousTerm = persistentState.currentTerm
        if (response.term > previousTerm) {
            persistentStateStore.save(response.term, null)
            persistentState.currentTerm = response.term
            persistentState.votedFor = null
            timer.reset(RaftTimeoutEvent.Election)
            return@withLock follower()
        }
        val progress = requireNotNull(peerProgress[peerId]) {
            "Unknown peer: $peerId"
        }
        val nextIndex = InstallSnapshotPolicy.nextIndexAfterSnapshot(request.metadata)
        peerProgress = peerProgress + (peerId to progress.copy(
            nextIndex = maxOf(progress.nextIndex, nextIndex),
            matchIndex = maxOf(progress.matchIndex, request.metadata.lastIncludedIndex),
        ))
        commitAdvancer.advance(peerProgress.values)
        raftApplier.applyCommitted()
        this
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
        if (response.term > previousTerm) {
            timer.reset(RaftTimeoutEvent.Election)
            follower()
        } else {
            this
        }
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
