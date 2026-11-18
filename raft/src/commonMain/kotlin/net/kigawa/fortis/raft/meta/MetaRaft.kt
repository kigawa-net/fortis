package net.kigawa.fortis.raft.meta

import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withTimeout
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.sharding.RangeId
import net.kigawa.fortis.raft.sharding.RangeMetadata
import net.kigawa.fortis.raft.snapshot.FileRaftSnapshotStore
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState

/** クラスタ管理専用の Raft グループ。RPC とタイマーは [runtime] に接続する。 */
class MetaRaft private constructor(
    val runtime: RaftRuntime,
    private val initialVoterId: String,
    private val stateMachine: MetaRaftStateMachine,
) {
    /** 最初の voter を選出し、初期構成をコミットする。 */
    suspend fun bootstrap(timeout: Duration = 5.seconds) {
        require(runtime.currentNode.nodeId == initialVoterId) { "Only the first voter can bootstrap" }
        if (localSnapshot().membership != null) return
        if (runtime.knownLeaderId() != initialVoterId) runtime.onElectionTimeout()
        submit(MetaCommand.Bootstrap(initialVoterId), timeout)
    }

    /** このノードに適用済みの状態を返す。 */
    suspend fun localSnapshot(): MetaRaftSnapshot = stateMachine.snapshot()

    /** リーダーの quorum を確認してから状態を読む。 */
    suspend fun snapshot(timeout: Duration = 5.seconds): MetaRaftSnapshot =
        runtime.linearizableRead(timeout) { stateMachine.snapshot() }

    suspend fun registerNode(node: RegisteredNode, timeout: Duration = 5.seconds) =
        submit(MetaCommand.Register(node), timeout)

    suspend fun updateNode(node: RegisteredNode, timeout: Duration = 5.seconds) =
        submit(MetaCommand.UpdateNode(node), timeout)

    suspend fun removeNode(nodeId: String, timeout: Duration = 5.seconds) =
        submit(MetaCommand.RemoveNode(nodeId), timeout)

    suspend fun putRange(range: RangeMetadata, timeout: Duration = 5.seconds) =
        submit(MetaCommand.PutRange(range), timeout)

    suspend fun splitRange(
        parent: RangeId,
        expectedEpoch: Long,
        key: ByteArray,
        left: RangeId,
        right: RangeId,
        timeout: Duration = 5.seconds,
    ) = submit(MetaCommand.Split(parent, expectedEpoch, key, left, right), timeout)

    suspend fun addLearner(nodeId: String, timeout: Duration = 5.seconds) =
        submit(MetaCommand.AddLearner(nodeId), timeout)

    /** snapshot と後続ログを転送し、learner が現在のログ末尾まで到達したかを返す。 */
    suspend fun catchUpLearner(nodeId: String, timeout: Duration = 5.seconds): Boolean {
        require(localSnapshot().membership?.members?.get(nodeId)?.role == ClusterMemberRole.LEARNER) {
            "Unknown learner: $nodeId"
        }
        require(timeout.isPositive() && timeout.isFinite())
        return withTimeout(timeout) { runtime.catchUpPeer(nodeId) }
    }

    /** 追いついた learner を投票構成に加える。 */
    suspend fun promoteLearner(nodeId: String, timeout: Duration = 5.seconds) {
        require(timeout.isPositive() && timeout.isFinite())
        withTimeout(timeout) {
            val state = localSnapshot()
            if (state.pendingPromotion != nodeId && state.membership?.members?.get(nodeId)?.isVoter() != true) {
                submit(MetaCommand.BeginPromotion(nodeId), timeout)
            }
            submit(MetaCommand.FinishPromotion(nodeId), timeout)
        }
    }

    /** 適用済み状態を永続化し、その境界までログを圧縮する。 */
    suspend fun saveSnapshot(): SnapshotMetadata = runtime.saveAppliedSnapshot { stateMachine.encodeSnapshot() }

    private suspend fun submit(command: MetaCommand, timeout: Duration) {
        runtime.propose(command.toRaftCommand(), timeout)
    }

    companion object {
        /** 各レプリカに同じ initialVoterId を指定する。永続構成ではログと snapshot store を渡す。 */
        suspend fun open(
            nodeId: String,
            initialVoterId: String,
            transport: RaftTransport,
            log: RaftLog = MemoryRaftLog(),
            persistentStateStore: RaftPersistentStateStore = MemoryRaftPersistentStateStore(),
            snapshotStore: FileRaftSnapshotStore? = null,
            timer: RaftTimer = RaftTimer.None,
            coroutineContext: CoroutineContext = Dispatchers.Default,
        ): MetaRaft {
            require(nodeId.isNotBlank() && initialVoterId.isNotBlank())
            val machine = MetaRaftStateMachine(initialVoterId)
            val node = RaftNodeBuilder(
                nodeId, emptySet(), persistentStateStore, RaftVolatileState(), log, machine, timer,
            ).build(snapshotStore, machine.snapshotApplier)
            val runtime = RaftRuntime(
                node, transport, snapshotStore, proposalCoroutineContext = coroutineContext,
                snapshotApplier = machine.snapshotApplier,
            )
            return MetaRaft(runtime, initialVoterId, machine)
        }
    }
}
