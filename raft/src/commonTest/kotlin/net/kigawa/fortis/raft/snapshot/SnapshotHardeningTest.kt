package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.*
import net.kigawa.fortis.raft.append.*
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.*
import net.kigawa.fortis.raft.node.*
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.*
import net.kigawa.fortis.raft.transport.codec.*
import net.kigawa.fortis.raft.vote.*
import kotlin.test.*

class SnapshotHardeningTest {
    private val boundary = SnapshotMetadata(3, 2)
    private fun entry(index: Long, term: Long = 2) =
        RaftLogEntry(index, term, RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(9)))

    @Test
    fun compactionWaitsForApplyAndKeepsSuffix() = runTest {
        val log = MemoryRaftLog()
        for (i in 1L..5L) log.append(entry(i))
        assertFailsWith<IllegalArgumentException> { log.compactThrough(boundary, 2) }
        assertNotNull(log.get(1))
        log.compactThrough(boundary, 3)
        assertNull(log.get(1))
        assertNull(log.get(3))
        assertEquals(2, log.termAt(3))
        assertEquals(entry(4), log.get(4))
        assertEquals(5, log.lastIndex())
        log.compactThrough(boundary, 5)
        log.truncateFrom(4)
        assertEquals(3, log.lastIndex())
        log.append(entry(4))
        assertEquals(4, log.lastIndex())
    }

    @Test
    fun mismatchingBoundaryDropsSuffixAndPreservesIndex() = runTest {
        val log = MemoryRaftLog()
        for (i in 1L..5L) log.append(entry(i))
        log.compactThrough(SnapshotMetadata(3, 4), 3)
        assertEquals(3, log.lastIndex())
        assertNull(log.get(4))
        log.append(entry(4, 4))
        assertEquals(4, log.termAt(4))
    }

    @Test
    fun boundaryParticipatesInAppendAndElection() = runTest {
        val log = MemoryRaftLog()
        log.compactThrough(boundary, 3)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(2))
        val applied = mutableListOf<RaftCommand>()
        val state = RaftVolatileState(commitIndex = 3, lastApplied = 3)
        val node = RaftNodeBuilder("f", setOf("l"), store, state, log, machine(applied))
        // 既存境界を回復済みのノードを直接構築する。
        val follower = FollowerNode("f", setOf("l"), store.load(), store, state, log, node.stateMachine, RaftTimer.None)
        assertFalse(follower.handleRequestVote(RequestVoteRequest(2, "l", 3, 1)).value.voteGranted)
        assertFalse(follower.handleAppendEntries(AppendEntriesRequest(2, "l", 3, 1, listOf(entry(4)), 4)).value.success)
        assertTrue(follower.handleAppendEntries(AppendEntriesRequest(2, "l", 3, 2, listOf(entry(4)), 4)).value.success)
        assertEquals(listOf(entry(4).command), applied)
        assertEquals(4, state.lastApplied)
    }

    @Test
    fun staleSnapshotCannotRollBackCommittedState() = runTest {
        val log = MemoryRaftLog()
        for (i in 1L..5L) log.append(entry(i))
        val state = RaftVolatileState(commitIndex = 5, lastApplied = 5)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(2))
        val handler = InstallSnapshotHandler(store.load(), store, state, log, RaftTimer.None,
            RaftSnapshotApplier { _, _ -> fail("Committed state must not be replaced") })
        handler.handle(InstallSnapshotRequest(2, "l", boundary, byteArrayOf(1)))
        assertNotNull(log.get(1))
        assertEquals(5, state.lastApplied)
    }

    @Test
    fun chunkRpcRoundTripAndMalformedFrames() {
        val codec = RaftRpcCodec()
        val chunks = InstallSnapshotChunks.chunked(2, "l", boundary, ByteArray(7) { it.toByte() }, 3)
        for (chunk in chunks) {
            val frame = codec.encode(RaftRpcMessage.SnapshotChunk(chunk))
            val decoded = assertIs<RaftRpcDecodeResult.Success>(codec.decode(frame))
            assertEquals(RaftRpcMessage.SnapshotChunk(chunk), decoded.message)
            assertIs<RaftRpcDecodeResult.Incomplete>(codec.decode(frame.copyOf(frame.size - 1)))
            frame[frame.lastIndex] = 3
            assertIs<RaftRpcDecodeResult.Corrupted>(codec.decode(frame))
        }
        val response = RaftRpcMessage.SnapshotChunkResult(InstallSnapshotChunkResponse(2, true, 7))
        assertEquals(response, assertIs<RaftRpcDecodeResult.Success>(codec.decode(codec.encode(response))).message)
    }

    @Test
    fun oversizedChunkLimitDoesNotOverflow() {
        val chunks = InstallSnapshotChunks.chunked(2, "l", boundary, ByteArray(5), Int.MAX_VALUE)
        assertEquals(1, chunks.size)
        assertTrue(chunks.single().isLast)
    }

    @Test
    fun runtimeChunkRpcInstallsThenAppendsSuffix() = runTest {
        val fixture = replicationFixture()
        fixture.leader.replicate()
        assertEquals(listOf(0L, 20L, 40L, 60L, 80L, 100L, 120L, 140L, 160L, 180L), fixture.transport.offsets)
        assertEquals(1, fixture.snapshots.size)
        assertContentEquals(ByteArray(200) { it.toByte() }, fixture.snapshots.single())
        assertNull(fixture.follower.currentNode.log.get(3))
        assertEquals(5, fixture.follower.currentNode.volatileState.lastApplied)
        assertEquals(listOf(entry(4).command, entry(5).command), fixture.applied)
        assertEquals(6, (fixture.leader.currentNode as LeaderNode).peerProgress.getValue("f").nextIndex)
        fixture.leader.stop()
        fixture.follower.stop()
    }

    @Test
    fun interruptedTransferDoesNotAdvanceProgressAndRetriesFromStart() = runTest {
        val f = replicationFixture()
        f.transport.failOffset = 20
        f.leader.replicate()
        assertTrue(f.snapshots.isEmpty())
        assertEquals(0, f.follower.currentNode.log.lastIndex())
        assertEquals(1, (f.leader.currentNode as LeaderNode).peerProgress.getValue("f").nextIndex)
        f.transport.failOffset = null
        f.leader.replicate()
        assertEquals(1, f.snapshots.size)
        assertEquals(5, f.follower.currentNode.log.lastIndex())
        f.leader.stop()
        f.follower.stop()
    }

    @Test
    fun higherTermMidTransferDemotesLeader() = runTest {
        val f = replicationFixture()
        f.transport.higherTermOffset = 20
        f.leader.replicate()
        assertIs<FollowerNode>(f.leader.currentNode)
        assertEquals(3, f.leader.currentNode.persistentState.currentTerm)
        assertTrue(f.snapshots.isEmpty())
        f.leader.stop()
        f.follower.stop()
    }

    @Test
    fun outOfOrderChunksAreRejectedAndNewStartRecovers() = runTest {
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(2))
        val snapshots = mutableListOf<ByteArray>()
        val handler = InstallSnapshotHandler(store.load(), store, RaftVolatileState(), MemoryRaftLog(), RaftTimer.None,
            RaftSnapshotApplier { _, bytes -> snapshots.add(bytes) })
        val chunks = InstallSnapshotChunks.chunked(2, "l", boundary, ByteArray(5), 2)
        assertFalse(handler.handleChunk(chunks[1]).accepted)
        assertTrue(handler.handleChunk(chunks[0]).accepted)
        assertFalse(handler.handleChunk(chunks[2]).accepted)
        assertTrue(snapshots.isEmpty())
        for (chunk in chunks) assertTrue(handler.handleChunk(chunk).accepted)
        assertEquals(1, snapshots.size)
    }

    private fun machine(applied: MutableList<RaftCommand>) = object : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) { applied.add(command) }
    }

    private suspend fun replicationFixture(): ReplicationFixture {
        val codec = RaftRpcCodec(maxPayloadLength = 100)
        val channel = InMemoryRaftRpcChannel(codec)
        val rpc = RpcRaftTransport(channel, codec)
        val transport = RecordingTransport(rpc)
        val applied = mutableListOf<RaftCommand>()
        val snapshots = mutableListOf<ByteArray>()
        val follower = RaftRuntime(RaftNodeBuilder("f", setOf("l"), MemoryRaftPersistentStateStore(),
            RaftVolatileState(), MemoryRaftLog(), machine(applied)).build(), rpc,
            snapshotApplier = RaftSnapshotApplier { _, bytes -> snapshots.add(bytes) })
        channel.register("f", RaftRpcHandler(follower))
        val log = MemoryRaftLog()
        for (i in 1L..5L) log.append(entry(i))
        log.compactThrough(boundary, 5)
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(2))
        val leaderNode = LeaderNode("l", setOf("f"), store.load(), store,
            RaftVolatileState(commitIndex = 5, lastApplied = 5), log, machine(mutableListOf()), RaftTimer.None,
            mapOf("f" to RaftPeerProgress(nextIndex = 1)))
        leaderNode.updateSnapshotMetadata(boundary)
        val leader = RaftRuntime(leaderNode, transport, snapshotStore = null, snapshotChunkBytes = 20,
            snapshotProvider = RaftSnapshotProvider { ByteArray(200) { it.toByte() } })
        return ReplicationFixture(leader, follower, transport, applied, snapshots)
    }

    private data class ReplicationFixture(val leader: RaftRuntime, val follower: RaftRuntime,
        val transport: RecordingTransport, val applied: List<RaftCommand>, val snapshots: List<ByteArray>)

    private class RecordingTransport(private val delegate: RaftTransport) : RaftTransport by delegate {
        val offsets = mutableListOf<Long>()
        var failOffset: Long? = null
        var higherTermOffset: Long? = null
        override suspend fun installSnapshotChunk(peerId: String, chunk: InstallSnapshotChunk): InstallSnapshotChunkResponse {
            offsets.add(chunk.offset)
            if (chunk.offset == failOffset) throw RaftTransportException("Interrupted")
            if (chunk.offset == higherTermOffset) return InstallSnapshotChunkResponse(3, false, 0)
            return delegate.installSnapshotChunk(peerId, chunk)
        }
    }
}
