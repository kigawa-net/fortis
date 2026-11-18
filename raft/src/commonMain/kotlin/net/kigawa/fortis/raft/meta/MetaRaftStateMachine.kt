package net.kigawa.fortis.raft.meta

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.sharding.KeyOrder
import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeMetadata
import net.kigawa.fortis.raft.sharding.RangeState
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.vote.RaftVotingConfiguration

internal class MetaRaftStateMachine(private val initialVoterId: String) : RaftStateMachine {
    private val mutex = Mutex()
    private var state = MetaRaftSnapshot(null, emptyMap(), emptyMap())

    val snapshotApplier = RaftSnapshotApplier { _, data ->
        val restored = decodeMetaSnapshot(data)
        require(restored.membership == null || initialVoterId in restored.membership.voters()) {
            "Meta Raft snapshot does not contain the initial voter"
        }
        mutex.withLock { state = restored }
    }

    suspend fun snapshot(): MetaRaftSnapshot = mutex.withLock {
        state.copy(
            membership = state.membership?.let { ClusterMembership(it.version, it.members) },
            nodes = state.nodes.toMap(),
            ranges = state.ranges.toMap(),
        )
    }

    suspend fun encodeSnapshot(): ByteArray = mutex.withLock { state.encode() }

    override suspend fun apply(command: RaftCommand) = mutex.withLock {
        state = nextState(command.toMetaCommand())
    }

    suspend fun members(): Set<String> = mutex.withLock {
        state.membership?.members?.keys?.toSet() ?: setOf(initialVoterId)
    }

    // A configuration takes effect locally as soon as it is appended. The joint
    // entry needs both majorities; the final entry can only follow a committed joint entry.
    suspend fun votingConfiguration(log: RaftLog, lastApplied: Long): RaftVotingConfiguration {
        val (appliedVoters, pending) = mutex.withLock {
            (state.membership?.voters() ?: setOf(initialVoterId)) to state.pendingPromotion
        }
        var current = appliedVoters
        var joining = pending
        for (index in lastApplied + 1..log.lastIndex()) {
            val payload = log.get(index)?.payload as? RaftLogEntryPayload.Command ?: continue
            if (!payload.command.isPromotion()) continue
            when (val command = payload.command.toMetaCommand()) {
                is MetaCommand.BeginPromotion -> if (command.nodeId !in current) joining = command.nodeId
                is MetaCommand.FinishPromotion -> if (joining == command.nodeId) {
                    current = current + command.nodeId
                    joining = null
                }
                else -> error("Expected a promotion entry")
            }
        }
        return RaftVotingConfiguration(current, joining?.let { current + it })
    }

    suspend fun validate(
        command: RaftCommand,
        log: RaftLog,
        lastApplied: Long,
        progress: Map<String, RaftPeerProgress>,
    ) {
        val lastIndex = log.lastIndex()
        for (index in lastApplied + 1..lastIndex) {
            check(log.get(index)?.payload !is RaftLogEntryPayload.Command) {
                "A Meta Raft command is awaiting commitment"
            }
        }
        val meta = command.toMetaCommand()
        mutex.withLock {
            if (meta is MetaCommand.BeginPromotion && state.pendingPromotion != meta.nodeId &&
                state.membership?.members?.get(meta.nodeId)?.isVoter() != true
            ) {
                check((progress[meta.nodeId]?.matchIndex ?: 0) >= lastIndex) {
                    "Learner ${meta.nodeId} has not caught up through index $lastIndex"
                }
            }
            nextState(meta)
        }
    }

    private fun nextState(command: MetaCommand): MetaRaftSnapshot {
        val config = state.membership
        if (command is MetaCommand.Bootstrap) {
            require(command.voterId == initialVoterId)
            return if (config == null) {
                state.copy(membership = ClusterMembership.initial(listOf(ClusterMember(initialVoterId))))
            } else {
                state
            }
        }
        checkNotNull(config) { "Meta Raft must be bootstrapped first" }
        return when (command) {
            is MetaCommand.Bootstrap -> error("Bootstrap was already handled")
            is MetaCommand.AddLearner -> {
                require(command.nodeId.isNotBlank())
                if (command.nodeId in config.members) {
                    state
                } else {
                    state.copy(membership = config.join(ClusterMember(
                        command.nodeId, ClusterMemberRole.LEARNER, ClusterMemberStatus.JOINING,
                    )))
                }
            }
            is MetaCommand.BeginPromotion -> {
                val member = requireNotNull(config.members[command.nodeId]) { "Unknown learner: ${command.nodeId}" }
                if (member.isVoter()) {
                    state
                } else {
                    check(state.pendingPromotion == null || state.pendingPromotion == command.nodeId) {
                        "Another promotion is in progress"
                    }
                    state.copy(pendingPromotion = command.nodeId)
                }
            }
            is MetaCommand.FinishPromotion -> {
                val member = requireNotNull(config.members[command.nodeId]) { "Unknown learner: ${command.nodeId}" }
                if (member.isVoter()) {
                    state
                } else {
                    check(state.pendingPromotion == command.nodeId) { "Joint configuration is not committed" }
                    state.copy(
                        membership = config.join(member.copy(
                            role = ClusterMemberRole.VOTER, status = ClusterMemberStatus.ACTIVE,
                        )),
                        pendingPromotion = null,
                    )
                }
            }
            is MetaCommand.Register -> {
                val node = command.node
                require(state.nodes[node.nodeId].let { it == null || it == node }) { "Node already registered" }
                state.copy(nodes = state.nodes + (node.nodeId to node))
            }
            is MetaCommand.UpdateNode -> {
                val node = command.node
                val previous = requireNotNull(state.nodes[node.nodeId]) { "Unknown node: ${node.nodeId}" }
                require(node.lastHeartbeat >= previous.lastHeartbeat) { "Node heartbeat moved backwards" }
                state.copy(nodes = state.nodes + (node.nodeId to node))
            }
            is MetaCommand.RemoveNode -> state.copy(nodes = state.nodes - command.nodeId)
            is MetaCommand.PutRange -> putRange(command.range)
            is MetaCommand.Split -> split(command)
        }
    }

    private fun putRange(range: RangeMetadata): MetaRaftSnapshot {
        val previous = state.ranges[range.rangeId]
        if (previous != null) {
            if (previous.epoch == range.epoch && previous.state == range.state && previous.keyRange == range.keyRange) {
                return state
            }
            require(previous.state != RangeState.DELETED && range.epoch > previous.epoch) {
                "Stale range epoch or deleted range: ${range.rangeId.value}"
            }
        }
        if (range.state != RangeState.DELETED) {
            require(state.ranges.values.none {
                it.rangeId != range.rangeId && it.state != RangeState.DELETED && overlaps(it.keyRange, range.keyRange)
            }) { "Range overlaps an existing range: ${range.rangeId.value}" }
        }
        return state.copy(ranges = state.ranges + (range.rangeId to range))
    }

    private fun split(command: MetaCommand.Split): MetaRaftSnapshot {
        val parent = requireNotNull(state.ranges[command.parent]) { "Unknown parent range: ${command.parent.value}" }
        require(parent.epoch == command.expectedEpoch) { "Stale parent epoch: ${parent.epoch}" }
        require(command.left !in state.ranges && command.right !in state.ranges) { "Split requires unused range IDs" }
        val splitting = if (parent.state == RangeState.SPLITTING) parent else parent.beginSplit()
        val (left, right) = splitting.splitAt(command.key, command.left, command.right)
        val deleted = splitting.deleted()
        return state.copy(ranges = state.ranges + listOf(
            deleted.rangeId to deleted, left.rangeId to left, right.rangeId to right,
        ))
    }

    private fun overlaps(left: KeyRange, right: KeyRange): Boolean =
        startsBeforeEnd(left.startInclusive, right.endExclusive) &&
            startsBeforeEnd(right.startInclusive, left.endExclusive)

    private fun startsBeforeEnd(start: ByteArray?, end: ByteArray?): Boolean =
        start == null || end == null || KeyOrder.compare(start, end) < 0
}
