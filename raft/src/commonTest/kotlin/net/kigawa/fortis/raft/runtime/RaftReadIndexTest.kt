package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class RaftReadIndexTest {
    @Test
    fun leaderReadsLatestCommittedValueThroughStorageStateMachine() = runTest {
        val fixture = fixture()
        fixture.runtime.propose(command(10))
        fixture.runtime.propose(command(20))
        val index = fixture.runtime.readIndex()
        assertEquals(RaftReadIndex(3, 1), index)
        val value = fixture.runtime.linearizableRead { (it as StorageEngineStateMachine).get(byteArrayOf(1)) }
        assertContentEquals(byteArrayOf(20), value)
        assertEquals(3L, fixture.state.lastApplied)
        assertEquals(3L, fixture.log.lastIndex()) // Reads add no log entry.
        assertEquals(2, fixture.transport.probeCalls.count { it.peerId == "node-2" })
        fixture.runtime.stop()
    }

    @Test
    fun followerRejectsReadWithoutCallingReaderOrSendingRpc() = runTest {
        val fixture = fixture(elect = false)
        var called = false
        assertFailsWith<RaftReadNotLeaderException> {
            fixture.runtime.linearizableRead { called = true }
        }
        assertFalse(called)
        assertEquals(emptyList(), fixture.transport.requests)
        fixture.runtime.stop()
    }

    @Test
    fun isolatedLeaderCannotReadEvenAfterPreviousMajorityCommit() = runTest {
        val fixture = fixture()
        fixture.runtime.propose(command(10))
        fixture.transport.unavailable = true
        var called = false
        val read = async {
            assertFailsWith<TimeoutCancellationException> {
                fixture.runtime.linearizableRead(100.milliseconds) { called = true }
            }
        }
        runCurrent()
        assertTrue(read.isActive)
        assertTrue(assertIs<LeaderNode>(fixture.runtime.currentNode).isReady)
        advanceTimeBy(100.milliseconds)
        runCurrent()
        read.await()
        assertFalse(called)
        fixture.runtime.stop()
    }

    @Test
    fun higherTermProbeResponseDemotesStaleLeaderWithoutReading() = runTest {
        val fixture = fixture()
        fixture.transport.probeTerm = 2
        var called = false
        val failure = assertFailsWith<RaftReadLeadershipLostException> {
            fixture.runtime.linearizableRead { called = true }
        }
        assertEquals(1L, failure.term)
        assertFalse(called)
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(2L, fixture.store.load().currentTerm)
        fixture.runtime.stop()
    }

    @Test
    fun singleNodeConfirmsOwnLeadershipWithoutRpc() = runTest {
        val fixture = fixture(peers = emptySet())
        assertEquals(RaftReadIndex(1, 1), fixture.runtime.readIndex())
        fixture.runtime.propose(command(10))
        assertContentEquals(byteArrayOf(10), fixture.runtime.linearizableRead {
            (it as StorageEngineStateMachine).get(byteArrayOf(1))
        })
        assertEquals(emptyList(), fixture.transport.requests)
        fixture.runtime.stop()
    }

    @Test
    fun readEstablishesCurrentTermCommitBeforeSendingConfirmationProbes() = runTest {
        val fixture = fixture(ready = false)
        assertFalse(assertIs<LeaderNode>(fixture.runtime.currentNode).isReady)
        assertEquals(0L, fixture.state.commitIndex)
        assertEquals(RaftReadIndex(1, 1), fixture.runtime.readIndex())
        assertTrue(assertIs<LeaderNode>(fixture.runtime.currentNode).isReady)
        assertTrue(fixture.transport.requests.first().request.entries.isNotEmpty())
        assertTrue(fixture.transport.probeCalls.isNotEmpty())
        assertEquals(1L, fixture.state.lastApplied)
        assertEquals(emptyMap(), fixture.engine.values)
        fixture.runtime.stop()
    }

    @Test
    fun healthyMajorityReturnsReadWithoutWaitingForSlowPeer() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        val read = async { fixture.runtime.readIndex() }
        val probes = List(2) { fixture.transport.pending.receive() }.associateBy { it.peerId }
        probes.getValue("node-3").response.complete(AppendEntriesResponse(1, true))
        runCurrent()
        assertTrue(read.isCompleted)
        assertEquals(RaftReadIndex(1, 1), read.await())
        assertEquals(0, fixture.transport.inFlight.values.sum())
        fixture.runtime.stop()
    }

    @Test
    fun readWaitsForDelayedLocalApplyBeforeReturningValue() = runTest {
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        val fixture = fixture(beforeApply = { entered.complete(Unit); release.await() })
        val proposal = async { fixture.runtime.propose(command(10)) }
        runCurrent()
        assertTrue(entered.isCompleted)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        var called = false
        val read = async {
            fixture.runtime.linearizableRead { called = true; (it as StorageEngineStateMachine).get(byteArrayOf(1)) }
        }
        runCurrent()
        assertTrue(read.isActive)
        assertFalse(called)
        release.complete(Unit)
        proposal.await()
        assertContentEquals(byteArrayOf(10), read.await())
        assertEquals(2L, fixture.state.lastApplied)
        fixture.runtime.stop()
    }

    @Test
    fun confirmedReadIndexStillWaitsForUnappliedCommit() = runTest {
        var failApply = true
        val fixture = fixture(beforeApply = { if (failApply) error("apply failed") })
        assertFailsWith<IllegalStateException> { fixture.runtime.propose(command(10)) }
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        var called = false
        val read = async {
            fixture.runtime.linearizableRead { called = true; (it as StorageEngineStateMachine).get(byteArrayOf(1)) }
        }
        runCurrent()
        assertTrue(fixture.transport.probeCalls.isNotEmpty())
        assertTrue(read.isActive)
        assertFalse(called)
        failApply = false
        fixture.runtime.replicate()
        assertContentEquals(byteArrayOf(10), read.await())
        assertEquals(2L, fixture.state.lastApplied)
        fixture.runtime.stop()
    }

    @Test
    fun readTimeoutDuringApplyDoesNotInterruptAcceptedWrite() = runTest {
        val release = CompletableDeferred<Unit>()
        val fixture = fixture(beforeApply = { release.await() })
        val proposal = async { fixture.runtime.propose(command(10)) }
        runCurrent()
        var called = false
        val read = async {
            assertFailsWith<TimeoutCancellationException> {
                fixture.runtime.linearizableRead(100.milliseconds) { called = true }
            }
        }
        runCurrent()
        advanceTimeBy(100.milliseconds)
        runCurrent()
        assertTrue(read.isCompleted)
        read.await()
        assertFalse(called)
        assertTrue(proposal.isActive)
        release.complete(Unit)
        proposal.await()
        runCurrent()
        assertEquals(2L, fixture.state.lastApplied)
        assertEquals(1, fixture.engine.putCount)
        assertEquals(emptyList(), fixture.transport.probeCalls)
        fixture.runtime.stop()
    }

    @Test
    fun olderHeartbeatResponsesCannotConfirmNewRead() = runTest {
        val fixture = fixture()
        fixture.transport.manualReplication = true
        val heartbeat = launch { fixture.runtime.onHeartbeatTimeout() }
        val old = List(2) { fixture.transport.pending.receive() }
        fixture.transport.manualProbes = true
        val read = async { fixture.runtime.readIndex() }
        runCurrent()
        assertTrue(fixture.transport.pending.tryReceive().isFailure)
        old.forEach { it.response.complete(AppendEntriesResponse(1, true)) }
        heartbeat.join()
        val fresh = List(2) { fixture.transport.pending.receive() }
        runCurrent()
        assertTrue(read.isActive)
        assertTrue(fresh.all { it.request.isProbe() })
        fresh.first().response.complete(AppendEntriesResponse(1, true))
        assertEquals(RaftReadIndex(1, 1), read.await())
        fixture.runtime.stop()
    }

    @Test
    fun concurrentReadsRequireIndependentFreshConfirmations() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        val first = async { fixture.runtime.readIndex() }
        val old = List(2) { fixture.transport.pending.receive() }
        val second = async { fixture.runtime.readIndex() }
        runCurrent()
        assertTrue(second.isActive)
        old.first().response.complete(AppendEntriesResponse(1, true))
        assertEquals(RaftReadIndex(1, 1), first.await())
        val fresh = List(2) { fixture.transport.pending.receive() }
        old.last().response.complete(AppendEntriesResponse(1, true))
        runCurrent()
        assertTrue(second.isActive)
        fresh.first().response.complete(AppendEntriesResponse(1, true))
        assertEquals(RaftReadIndex(1, 1), second.await())
        assertTrue(fixture.transport.maximumInFlight.values.all { it == 1 })
        fixture.runtime.stop()
    }

    @Test
    fun cancellationReleasesProbeLocksAndLateResponseCannotConfirmNextRead() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        val first = async { fixture.runtime.readIndex() }
        val old = List(2) { fixture.transport.pending.receive() }
        first.cancelAndJoin()
        runCurrent()
        assertEquals(0, fixture.transport.inFlight.values.sum())
        val next = async { fixture.runtime.readIndex() }
        val fresh = List(2) { fixture.transport.pending.receive() }
        old.forEach { it.response.complete(AppendEntriesResponse(1, true)) }
        runCurrent()
        assertTrue(next.isActive)
        fresh.first().response.complete(AppendEntriesResponse(1, true))
        next.await()
        fixture.runtime.stop()
    }

    @Test
    fun demotionAfterQuorumBeforeReaderRunsIsRechecked() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        var called = false
        val read = async {
            assertFailsWith<RaftReadLeadershipLostException> {
                fixture.runtime.linearizableRead { called = true }
            }
        }
        val probes = List(2) { fixture.transport.pending.receive() }
        // Both workers are queued before the caller can resume from quorum completion.
        probes.first().response.complete(AppendEntriesResponse(1, true))
        probes.last().response.complete(AppendEntriesResponse(2, false))
        runCurrent()
        read.await()
        assertFalse(called)
        assertEquals(2L, fixture.store.load().currentTerm)
        fixture.runtime.stop()
    }

    @Test
    fun lowerTermAndRejectedResponsesDoNotCountTowardReadQuorum() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        val read = async {
            assertFailsWith<TimeoutCancellationException> { fixture.runtime.readIndex(100.milliseconds) }
        }
        val probes = List(2) { fixture.transport.pending.receive() }
        probes.first().response.complete(AppendEntriesResponse(0, true))
        probes.last().response.complete(AppendEntriesResponse(1, false))
        runCurrent()
        assertTrue(read.isActive)
        advanceTimeBy(100.milliseconds)
        runCurrent()
        read.await()
        fixture.runtime.stop()
    }

    @Test
    fun cancellationBeforeSubmissionDoesNotSendConfirmationRpc() = runTest {
        val fixture = fixture()
        val read = async(start = CoroutineStart.UNDISPATCHED) { fixture.runtime.readIndex() }
        read.cancelAndJoin()
        runCurrent()
        assertEquals(emptyList(), fixture.transport.probeCalls)
        assertEquals(RaftReadIndex(1, 1), fixture.runtime.readIndex())
        fixture.runtime.stop()
    }

    @Test
    fun stoppingRuntimeFailsPendingAndNewReads() = runTest {
        val fixture = fixture()
        fixture.transport.manualProbes = true
        val read = async { assertFailsWith<RaftReadStoppedException> { fixture.runtime.readIndex() } }
        List(2) { fixture.transport.pending.receive() }
        fixture.runtime.stop()
        read.await()
        assertFailsWith<RaftReadStoppedException> { fixture.runtime.readIndex() }
        assertEquals(0, fixture.transport.inFlight.values.sum())
    }

    @Test
    fun timeoutIncludesReaderAndReleasesStateLock() = runTest {
        val fixture = fixture()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val read = async {
            assertFailsWith<TimeoutCancellationException> {
                fixture.runtime.linearizableRead(100.milliseconds) { entered.complete(Unit); release.await() }
            }
        }
        runCurrent()
        assertTrue(entered.isCompleted)
        advanceTimeBy(100.milliseconds)
        runCurrent()
        read.await()
        assertTrue(fixture.runtime.handleRequestVote(RequestVoteRequest(2, "node-2", 1, 1)).voteGranted)
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        fixture.runtime.stop()
    }

    @Test
    fun stopDuringReaderCannotReturnSuccessfulValue() = runTest {
        val fixture = fixture()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val read = async {
            assertFailsWith<RaftReadStoppedException> {
                fixture.runtime.linearizableRead { entered.complete(Unit); release.await(); 42 }
            }
        }
        runCurrent()
        assertTrue(entered.isCompleted)
        val stopping = launch { fixture.runtime.stop() }
        runCurrent()
        release.complete(Unit)
        read.await()
        stopping.join()
    }

    private suspend fun TestScope.fixture(
        peers: Set<String> = setOf("node-2", "node-3"),
        elect: Boolean = true,
        ready: Boolean = true,
        beforeApply: suspend () -> Unit = {},
    ): Fixture {
        val store = MemoryRaftPersistentStateStore()
        val state = RaftVolatileState()
        val log = MemoryRaftLog()
        val engine = Engine(beforeApply)
        val transport = Transport()
        val node = RaftNodeBuilder("node-1", peers, store, state, log, StorageEngineStateMachine(engine)).build()
        val runtime = RaftRuntime(node, transport, backgroundScope.coroutineContext)
        if (elect) {
            runtime.onElectionTimeout()
            if (ready) runtime.replicate()
        }
        return Fixture(runtime, transport, state, log, store, engine)
    }

    private data class Fixture(
        val runtime: RaftRuntime,
        val transport: Transport,
        val state: RaftVolatileState,
        val log: MemoryRaftLog,
        val store: MemoryRaftPersistentStateStore,
        val engine: Engine,
    )

    private class Engine(private val beforeApply: suspend () -> Unit) : FortisStorageEngine {
        val values = mutableMapOf<Byte, ByteArray>()
        var putCount = 0
        override suspend fun get(key: ByteArray): ByteArray? = values[key.single()]?.copyOf()
        override suspend fun put(key: ByteArray, value: ByteArray) {
            beforeApply()
            values[key.single()] = value.copyOf()
            putCount++
        }
        override suspend fun delete(key: ByteArray): Boolean = values.remove(key.single()) != null
    }

    private class Transport : RaftTransport {
        val requests = mutableListOf<Pending>()
        val probeCalls get() = requests.filter { it.request.isProbe() }
        val pending = Channel<Pending>(Channel.UNLIMITED)
        var unavailable = false
        var manualProbes = false
        var manualReplication = false
        var probeTerm: Long? = null
        val inFlight = mutableMapOf<String, Int>()
        val maximumInFlight = mutableMapOf<String, Int>()
        override suspend fun requestVote(peerId: String, request: RequestVoteRequest) = RequestVoteResponse(request.term, true)
        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse {
            val call = Pending(peerId, request)
            requests.add(call)
            val active = inFlight.getOrElse(peerId) { 0 } + 1
            inFlight[peerId] = active
            maximumInFlight[peerId] = maxOf(maximumInFlight.getOrElse(peerId) { 0 }, active)
            try {
                if (unavailable) throw RaftPeerUnavailableException(peerId)
                if ((request.isProbe() && manualProbes) || (!request.isProbe() && manualReplication)) {
                    pending.send(call)
                    return call.response.await()
                }
                val term = if (request.isProbe()) probeTerm ?: request.term else request.term
                return AppendEntriesResponse(term, term == request.term)
            } finally {
                inFlight[peerId] = inFlight.getValue(peerId) - 1
            }
        }
    }

    private data class Pending(
        val peerId: String,
        val request: AppendEntriesRequest,
        val response: CompletableDeferred<AppendEntriesResponse> = CompletableDeferred(),
    )

    private fun command(value: Int) = RaftCommand.Put(byteArrayOf(1), byteArrayOf(value.toByte()))
}

private fun AppendEntriesRequest.isProbe() = entries.isEmpty() && prevLogIndex == 0L && leaderCommit == 0L
