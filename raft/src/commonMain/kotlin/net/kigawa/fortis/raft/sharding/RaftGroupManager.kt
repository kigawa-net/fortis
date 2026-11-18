package net.kigawa.fortis.raft.sharding

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.runtime.RaftReadLeadershipLostException
import net.kigawa.fortis.raft.runtime.RaftReadNotLeaderException
import net.kigawa.fortis.raft.runtime.RaftProposalNotLeaderException
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.scan.ScanEntry
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.codec.GroupRaftRpcMessage
import net.kigawa.fortis.raft.transport.codec.senderId
import net.kigawa.fortis.storage.engine.mvcc.MvccScanResult

/** 同一ノードのレンジ別 Raft ランタイムを管理する。 */
class RaftGroupManager(
    val nodeId: String,
    private val peerResolver: RaftPeerResolver,
) {
    private val registryLock = Mutex()
    private val groups = mutableMapOf<RangeId, Group>()
    private val starting = mutableMapOf<RangeId, Group>()
    private val tombstones = mutableMapOf<RangeId, RangeMetadata>()

    /** レンジ専用のログ・永続状態・スナップショット・状態機械を持つランタイムを起動する。 */
    suspend fun create(metadata: RangeMetadata, runtime: RaftRuntime) {
        require(metadata.state == RangeState.ACTIVE)
        val node = runtime.currentNode
        require(node.nodeId == nodeId) { "runtime belongs to another node" }
        val id = metadata.rangeId
        val group = Group(metadata, runtime)
        registryLock.withLock {
            require(id !in groups && id !in tombstones && id !in starting) { "range ID already registered: $id" }
            for (other in groups.values + starting.values) {
                val otherNode = other.runtime.currentNode
                require(node.log !== otherNode.log && node.stateMachine !== otherNode.stateMachine &&
                    node.persistentStateStore !== otherNode.persistentStateStore &&
                    node.volatileState !== otherNode.volatileState) { "range groups must own independent Raft state" }
            }
            starting[id] = group
        }
        try {
            runtime.start()
            registryLock.withLock { groups[id] = group }
        } catch (failure: Throwable) {
            withContext(NonCancellable) {
                try {
                    runtime.stop()
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressed(cleanupFailure)
                }
            }
            throw failure
        } finally {
            withContext(NonCancellable) { registryLock.withLock { starting.remove(id) } }
        }
    }

    /** 分割開始を反映し、この世代から親レンジへの KV アクセスを停止する。 */
    suspend fun beginSplit(route: RangeRoute): RangeMetadata = change(route, RangeMetadata::beginSplit)

    /** 再配置の切り替えを反映する。 */
    suspend fun advancePlacementEpoch(route: RangeRoute): RangeMetadata = change(route, RangeMetadata::relocated)

    private suspend fun change(route: RangeRoute, update: (RangeMetadata) -> RangeMetadata): RangeMetadata =
        withRange(route) { group -> update(group.metadata).also { group.metadata = it } }

    /** グループを停止し、廃止した ID と最終世代を保持する。 */
    suspend fun delete(route: RangeRoute): RangeMetadata {
        val group = find(route)
        return group.clientLock.withLock {
            group.metadata.checkEpoch(route)
            if (group.metadata.state == RangeState.DELETED) throw RangeUnavailableException(route.rangeId)
            val deleted = group.metadata.deleted()
            group.rpcLock.withLock {
                withContext(NonCancellable) {
                    group.retired = true
                    group.metadata = deleted
                    registryLock.withLock {
                        tombstones[route.rangeId] = deleted
                    }
                    try {
                        group.runtime.stop()
                    } finally {
                        registryLock.withLock { groups.remove(route.rangeId) }
                    }
                }
            }
            deleted
        }
    }

    /** 担当レンジの quorum を確認して値を読む。 */
    suspend fun get(route: RangeRoute, key: ByteArray): RangeResponse<ByteArray?> {
        val target = key.copyOf()
        return withRange(route) { group ->
            group.metadata.requireKey(target)
            responding(group) { group.runtime.linearizableRead { (it as StorageEngineStateMachine).get(target) } }
        }
    }

    suspend fun put(route: RangeRoute, key: ByteArray, value: ByteArray): RangeResponse<RaftLogEntry> =
        RaftCommand.Put(key, value).let { command -> propose(route, command, command.key) }

    suspend fun delete(route: RangeRoute, key: ByteArray): RangeResponse<RaftLogEntry> =
        RaftCommand.Delete(key).let { command -> propose(route, command, command.key) }

    private suspend fun propose(route: RangeRoute, command: RaftCommand, key: ByteArray): RangeResponse<RaftLogEntry> {
        return withRange(route) { group ->
            group.metadata.requireKey(key)
            responding(group) { group.runtime.propose(command) }
        }
    }

    /** shard 内の指定区間を quorum 確認後の revision で読む。 */
    suspend fun scan(slice: RangeScanSlice, limit: Int): RangeResponse<List<ScanEntry>> {
        require(limit > 0) { "scan limit must be positive" }
        return withRange(slice.route) { group ->
            group.metadata.requireBounds(slice.bounds)
            responding(group) {
                group.runtime.linearizableRead { (it as StorageEngineStateMachine).scanRange(slice.bounds, limit) }
            }
        }
    }

    /** 指定グループのピアから届いた Raft RPC を処理する。 */
    suspend fun handleRpc(request: GroupRaftRpcMessage): GroupRaftRpcMessage {
        val group = registryLock.withLock { groups[RangeId(request.groupId)] }
            ?: throw RaftTransportException("Unknown Raft group: ${request.groupId}")
        return group.rpcLock.withLock {
            val sender = request.message.senderId()
            if (group.retired || !group.handler.isKnownPeer(sender)) {
                throw RaftTransportException("Raft peer $sender is unavailable in group ${request.groupId}")
            }
            GroupRaftRpcMessage(request.groupId, group.handler.handle(request.message))
        }
    }

    private suspend fun <T> responding(group: Group, operation: suspend () -> T): RangeResponse<T> = try {
        RangeResponse.Success(operation())
    } catch (failure: RaftProposalNotLeaderException) {
        redirect(group)
    } catch (failure: RaftReadNotLeaderException) {
        redirect(group)
    } catch (failure: RaftReadLeadershipLostException) {
        redirect(group)
    }

    private suspend fun redirect(group: Group): RangeResponse<Nothing> {
        val metadata = group.metadata
        val route = RangeRoute(GroupHandle(metadata.rangeId.value), metadata.epoch)
        val leaderId = group.runtime.knownLeaderId()
        if (leaderId == null || leaderId == nodeId) return RangeResponse.LeaderUnavailable(route)
        return RedirectResponse(route, leaderId, peerResolver.resolve(leaderId))
    }

    private suspend fun <T> withRange(route: RangeRoute, operation: suspend (Group) -> T): T {
        val group = find(route)
        return group.clientLock.withLock {
            group.metadata.checkEpoch(route)
            if (group.metadata.state != RangeState.ACTIVE) throw RangeUnavailableException(route.rangeId)
            operation(group)
        }
    }

    private suspend fun find(route: RangeRoute): Group = registryLock.withLock {
        groups[route.rangeId] ?: run {
            tombstones[route.rangeId]?.checkEpoch(route)
            throw RangeUnavailableException(route.rangeId)
        }
    }

    private fun RangeMetadata.checkEpoch(route: RangeRoute) {
        if (epoch != route.epoch) throw StaleRangeEpochException(rangeId, route.epoch, epoch)
    }

    private fun RangeMetadata.requireKey(key: ByteArray) {
        require(keyRange.contains(key)) { "key is outside range ${rangeId.value}" }
    }

    private fun RangeMetadata.requireBounds(bounds: KeyRange) {
        val start = bounds.startInclusive
        val end = bounds.endExclusive
        val lower = keyRange.startInclusive
        val upper = keyRange.endExclusive
        require(lower == null || (start != null && KeyOrder.compare(lower, start) <= 0)) {
            "scan starts outside range ${rangeId.value}"
        }
        require(upper == null || (end != null && KeyOrder.compare(end, upper) <= 0)) {
            "scan ends outside range ${rangeId.value}"
        }
    }

    private class Group(var metadata: RangeMetadata, val runtime: RaftRuntime) {
        val clientLock = Mutex()
        val rpcLock = Mutex()
        var retired = false // Protected by rpcLock.
        val handler = RaftRpcHandler(runtime)
    }
}

private suspend fun StorageEngineStateMachine.scanRange(bounds: KeyRange, limit: Int): List<ScanEntry> {
    val revision = latestRevision()
    val end = bounds.endExclusive
    val entries = mutableListOf<ScanEntry>()
    var after: ByteArray? = null
    do {
        val page = scanPrefixAt(byteArrayOf(), revision, after, 256)
        check(page is MvccScanResult.Page) { "Applied scan revision $revision was compacted" }
        for (entry in page.entries) {
            val key = entry.key
            if (end != null && KeyOrder.compare(key, end) >= 0) return entries
            if (bounds.contains(key)) {
                entries += ScanEntry(key, entry.value, entry.version)
                if (entries.size == limit) return entries
            }
        }
        after = page.entries.lastOrNull()?.key
    } while (page.hasMore)
    return entries
}
