package net.kigawa.fortis.raft.sharding

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.scan.ScanEntry
import net.kigawa.fortis.raft.transport.InMemoryRaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull

class MultiRaftIntegrationTest {
    private val left = RangeMetadata(KeyRange("left", null, key(128)), epoch = 4)
    private val right = RangeMetadata(KeyRange("right", key(128), null), epoch = 7)
    private val router = MultiRaftRouter.fromMetadata(listOf(right, left))
    private val addresses = mapOf(
        "n1" to RaftPeerAddress("127.0.0.1", 9001),
        "n2" to RaftPeerAddress("127.0.0.1", 9002),
    )
    private val resolver = RaftPeerResolver(addresses::getValue)

    @Test
    fun groupsElectIndependentlyRouteKvAndRedirectToTheirOwnLeaders() = runTest {
        val channel = InMemoryRaftRpcChannel()
        val first = RaftGroupManager("n1", resolver)
        val second = RaftGroupManager("n2", resolver)
        channel.registerGroupManager(first)
        channel.registerGroupManager(second)
        val leftOnFirst = runtime(first, left, channel, setOf("n2"))
        val leftOnSecond = runtime(second, left, channel, setOf("n1"))
        val rightOnFirst = runtime(first, right, channel, setOf("n2"))
        val rightOnSecond = runtime(second, right, channel, setOf("n1"))

        val a = key(127)
        val b = key(128)
        val leftRoute = router.routeWithEpoch(a)
        val rightRoute = router.routeWithEpoch(b)
        assertEquals(RangeRoute(GroupHandle(left.rangeId.value), left.epoch), leftRoute)
        assertEquals(RangeRoute(GroupHandle(right.rangeId.value), right.epoch), rightRoute)
        assertIs<RangeResponse.LeaderUnavailable>(first.put(leftRoute, a, a))
        assertFailsWith<IllegalArgumentException> { first.put(leftRoute, b, b) }

        leftOnFirst.onElectionTimeout()
        assertEquals(0L, rightOnFirst.currentNode.log.lastIndex())
        rightOnSecond.onElectionTimeout()
        assertIs<LeaderNode>(leftOnFirst.currentNode)
        assertIs<LeaderNode>(rightOnSecond.currentNode)

        val leftEntry = assertIs<RangeResponse.Success<*>>(first.put(leftRoute, a, b)).value
        val rightEntry = assertIs<RangeResponse.Success<*>>(second.put(rightRoute, b, a)).value
        assertEquals(leftEntry, leftOnSecond.currentNode.log.get(2))
        assertEquals(rightEntry, rightOnFirst.currentNode.log.get(2))
        assertContentEquals(b, assertIs<RangeResponse.Success<ByteArray?>>(first.get(leftRoute, a)).value)
        assertContentEquals(a, assertIs<RangeResponse.Success<ByteArray?>>(second.get(rightRoute, b)).value)
        assertEquals(RedirectResponse(rightRoute, "n2", addresses.getValue("n2")), first.delete(rightRoute, b))
        assertEquals(RedirectResponse(leftRoute, "n1", addresses.getValue("n1")), second.get(leftRoute, a))

        // A higher term invalidates the old address and a delayed heartbeat cannot restore it.
        leftOnSecond.onElectionTimeout()
        assertIs<RangeResponse.LeaderUnavailable>(first.get(leftRoute, a))
        RpcRaftTransport.forGroup("left", channel).appendEntries(
            "n1", AppendEntriesRequest(1, "n2", 0, 0, emptyList(), 0),
        )
        assertIs<RangeResponse.LeaderUnavailable>(first.get(leftRoute, a))
        leftOnSecond.onHeartbeatTimeout()
        val redirect = assertIs<RedirectResponse>(first.delete(leftRoute, a))
        assertEquals(addresses.getValue("n2"), redirect.leaderAddress)
        second.delete(redirect.route, a)
        assertNull(assertIs<RangeResponse.Success<ByteArray?>>(second.get(leftRoute, a)).value)
        assertContentEquals(a, assertIs<RangeResponse.Success<ByteArray?>>(second.get(rightRoute, b)).value)

        // Removing one group on a peer loses only that group's quorum.
        val tombstone = first.delete(leftRoute)
        assertEquals(RangeState.DELETED, tombstone.state)
        assertFailsWith<StaleRangeEpochException> { first.get(leftRoute, a) }
        assertFailsWith<RangeUnavailableException> { first.get(leftRoute.copy(epoch = tombstone.epoch), a) }
        assertEquals(RedirectResponse(rightRoute, "n2", addresses.getValue("n2")), first.get(rightRoute, b))
        assertFailsWith<TimeoutCancellationException> { second.put(leftRoute, a, a) }
        assertIs<RangeResponse.Success<*>>(second.put(rightRoute, b, b))
    }

    @Test
    fun relocationAndSplitFenceOldRequestsAndCreateFreshGroups() = runTest {
        val channel = InMemoryRaftRpcChannel()
        val manager = RaftGroupManager("n1", resolver)
        channel.registerGroupManager(manager)
        val timer = RecordingTimer()
        val parent = RangeMetadata(KeyRange("parent", null, null))
        val initial = MultiRaftRouter.fromMetadata(listOf(parent))
        val oldRoute = initial.routeWithEpoch(key(1))
        val parentRuntime = runtime(manager, parent, channel, timer = timer)
        assertEquals(RaftTimeoutEvent.Election, timer.lastEvent)
        assertFailsWith<IllegalArgumentException> {
            manager.create(RangeMetadata(KeyRange("other", null, null)), parentRuntime)
        }
        parentRuntime.onElectionTimeout()
        val relocated = manager.advancePlacementEpoch(oldRoute)
        assertEquals(1L, relocated.epoch)
        val stale = assertFailsWith<StaleRangeEpochException> { manager.put(oldRoute, key(1), key(9)) }
        assertEquals(parent.rangeId, stale.rangeId)
        assertEquals(oldRoute.epoch, stale.supplied)
        assertEquals(relocated.epoch, stale.current)
        assertEquals(1L, parentRuntime.currentNode.log.lastIndex())

        val relocatedRoute = initial.routeWithEpoch(key(1)).copy(epoch = relocated.epoch)
        val splitting = manager.beginSplit(relocatedRoute)
        assertEquals(2L, splitting.epoch)
        assertEquals(RangeState.SPLITTING, splitting.state)
        val splittingRoute = MultiRaftRouter.fromMetadata(listOf(splitting)).routeWithEpoch(key(1))
        assertFailsWith<StaleRangeEpochException> { manager.delete(relocatedRoute, key(1)) }
        assertFailsWith<RangeUnavailableException> { manager.get(splittingRoute, key(1)) }
        assertFailsWith<IllegalArgumentException> { splitting.splitAt(byteArrayOf(), left.rangeId, right.rangeId) }
        val (lower, upper) = splitting.splitAt(key(128), left.rangeId, right.rangeId)
        assertEquals(3L, lower.epoch)
        assertEquals(3L, upper.epoch)
        runtime(manager, lower, channel).onElectionTimeout()
        runtime(manager, upper, channel).onElectionTimeout()
        val deleted = manager.delete(splittingRoute)
        assertEquals(3L, deleted.epoch)
        assertNull(timer.lastEvent)
        assertFailsWith<IllegalArgumentException> { manager.create(parent, parentRuntime) }
        assertFailsWith<RaftTransportException> {
            RpcRaftTransport.forGroup("parent", channel).appendEntries(
                "n1", AppendEntriesRequest(2, "n2", 0, 0, emptyList(), 0),
            )
        }

        val updated = MultiRaftRouter.fromMetadata(listOf(deleted, lower, upper))
        assertEquals(lower.rangeId, updated.routeWithEpoch(key(127)).rangeId)
        assertEquals(upper.rangeId, updated.routeWithEpoch(key(128)).rangeId)
        assertIs<RangeResponse.Success<*>>(manager.put(updated.routeWithEpoch(key(127)), key(127), key(1)))
        assertFailsWith<StaleRangeEpochException> { manager.put(oldRoute, key(127), key(1)) }
    }

    @Test
    fun scanClipsShardQueriesMergesUnsignedOrderAndHonorsLimit() = runTest {
        val channel = InMemoryRaftRpcChannel()
        val manager = RaftGroupManager("n1", resolver)
        runtime(manager, left, channel).onElectionTimeout()
        runtime(manager, right, channel).onElectionTimeout()
        for (value in listOf(255, 128, 1, 254, 127, 0, 126)) {
            val key = key(value)
            manager.put(router.routeWithEpoch(key), key, key)
        }
        manager.delete(router.routeWithEpoch(key(126)), key(126))

        val slices = router.planScan(key(1), key(255))
        assertEquals(listOf(left.rangeId, right.rangeId), slices.map { it.route.rangeId })
        assertContentEquals(key(1), slices.first().bounds.startInclusive)
        assertContentEquals(key(128), slices.first().bounds.endExclusive)
        assertContentEquals(key(128), slices.last().bounds.startInclusive)
        assertContentEquals(key(255), slices.last().bounds.endExclusive)
        assertEquals(listOf(left.rangeId), router.planScan(null, key(128)).map { it.route.rangeId })
        assertEquals(emptyList(), router.planScan(null, byteArrayOf()))

        val remaining = mutableListOf<Int>()
        val result = router.scan(key(1), key(255), limit = 3) { slice, limit ->
            remaining += limit
            manager.scan(slice, limit)
        }
        assertEquals(listOf(3, 1), remaining)
        val page = assertIs<RangeResponse.Success<List<ScanEntry>>>(result)
        assertEquals(listOf(1, 127, 128), page.value.map { it.key.single().toUByte().toInt() })

        val full = assertIs<RangeResponse.Success<List<ScanEntry>>>(router.scan(null, null, 20, manager::scan))
        assertEquals(listOf(0, 1, 127, 128, 254, 255), full.value.map { it.key.single().toUByte().toInt() })
        val redirect = RedirectResponse(slices.last().route, "n2", addresses.getValue("n2"))
        assertEquals(redirect, router.scan(null, null, 20) { slice, limit ->
            if (slice.route.rangeId == right.rangeId) redirect else manager.scan(slice, limit)
        })
        assertFailsWith<IllegalArgumentException> {
            manager.scan(RangeScanSlice(slices.first().route, KeyRange("left", null, key(255))), 20)
        }
    }

    private suspend fun TestScope.runtime(
        manager: RaftGroupManager,
        metadata: RangeMetadata,
        channel: InMemoryRaftRpcChannel,
        peers: Set<String> = emptySet(),
        timer: RaftTimer = RaftTimer.None,
    ): RaftRuntime {
        val node = RaftNodeBuilder(
            manager.nodeId, peers, MemoryRaftPersistentStateStore(), RaftVolatileState(),
            MemoryRaftLog(), StorageEngineStateMachine(MemoryMvccStorageEngine()), timer,
        ).build()
        val transport = RpcRaftTransport.forGroup(metadata.rangeId.value, channel)
        return RaftRuntime(node, transport, proposalCoroutineContext = backgroundScope.coroutineContext).also {
            manager.create(metadata, it)
        }
    }

    private class RecordingTimer : RaftTimer {
        var lastEvent: RaftTimeoutEvent? = null
        override suspend fun reset(event: RaftTimeoutEvent) {
            lastEvent = event
        }
        override suspend fun cancel() {
            lastEvent = null
        }
    }

    private fun key(value: Int): ByteArray = byteArrayOf(value.toByte())
}
