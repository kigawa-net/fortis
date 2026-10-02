package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class RaftRuntimeConcurrencyTest {
    @Test
    fun healthyMajorityElectsLeaderBeforeDelayedPeerResponds() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        val election = launch { fixture.runtime.onElectionTimeout() }
        val pending = List(2) { transport.votes.receive() }.associateBy { it.peerId }
        pending.getValue("node-3").response.complete(RequestVoteResponse(1, true))
        runCurrent()
        assertIs<LeaderNode>(fixture.runtime.currentNode)
        assertTrue(election.isActive)
        pending.getValue("node-2").response.complete(RequestVoteResponse(1, false))
        election.join()
    }

    @Test
    fun delayedHigherTermVoteResponseDemotesNewlyElectedLeader() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        val election = launch { fixture.runtime.onElectionTimeout() }
        val pending = List(2) { transport.votes.receive() }.associateBy { it.peerId }
        pending.getValue("node-3").response.complete(RequestVoteResponse(1, true))
        runCurrent()
        assertIs<LeaderNode>(fixture.runtime.currentNode)
        pending.getValue("node-2").response.complete(RequestVoteResponse(4, false))
        election.join()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(4L, fixture.store.load().currentTerm)
        assertEquals(null, fixture.store.load().votedFor)
    }

    @Test
    fun healthyMajorityCommitsAndAppliesBeforeDelayedPeerResponds() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        elect(fixture.runtime, transport)
        val append = async { fixture.runtime.appendCommand(command(1)) }
        val pending = List(2) { transport.appends.receive() }.associateBy { it.peerId }
        pending.getValue("node-3").response.complete(AppendEntriesResponse(1, true))
        runCurrent()
        assertEquals(1L, fixture.state.commitIndex)
        assertEquals(listOf(command(1)), fixture.applied)
        assertTrue(append.isActive)
        pending.getValue("node-2").response.complete(AppendEntriesResponse(1, true))
        append.await()
    }

    @Test
    fun overlappingCommandsSerializeRpcAndUseUpdatedPeerProgress() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val first = async { fixture.runtime.appendCommand(command(1)) }
        val initial = transport.appends.receive()
        val second = async { fixture.runtime.appendCommand(command(2)) }
        runCurrent()
        assertEquals(2L, fixture.runtime.currentNode.log.lastIndex())
        assertTrue(transport.appends.tryReceive().isFailure)
        initial.response.complete(AppendEntriesResponse(1, true))
        val next = transport.appends.receive()
        assertEquals(1L, next.request.prevLogIndex)
        assertEquals(listOf(command(2)), next.request.entries.map { it.command })
        next.response.complete(AppendEntriesResponse(1, true))
        first.await()
        second.await()
        assertEquals(1, transport.maximumInFlight.getValue("node-2"))
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
    }

    @Test
    fun cancellationReleasesPeerRpcLockForNextCommand() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val first = launch { fixture.runtime.appendCommand(command(1)) }
        transport.appends.receive()
        first.cancelAndJoin()
        assertEquals(0, transport.inFlight.getValue("node-2"))
        val next = async { fixture.runtime.appendCommand(command(2)) }
        val pending = transport.appends.receive()
        pending.response.complete(AppendEntriesResponse(1, true))
        next.await()
        assertEquals(1, transport.maximumInFlight.getValue("node-2"))
        assertEquals(listOf(command(1), command(2)), fixture.applied)
    }

    @Test
    fun simultaneousElectionsDoNotWaitForEachOthersStateLocks() = runTest {
        val local = LocalRaftTransport()
        val bothSending = CompletableDeferred<Unit>()
        var sent = 0
        val transport = object : RaftTransport by local {
            override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse {
                if (++sent == 2) bothSending.complete(Unit)
                bothSending.await()
                return local.requestVote(peerId, request)
            }
        }
        val first = fixture(transport, "node-1", setOf("node-2"))
        val second = fixture(transport, "node-2", setOf("node-1"))
        local.register("node-1", first.runtime)
        local.register("node-2", second.runtime)
        withTimeout(1.seconds) {
            val elections = listOf(
                async { first.runtime.onElectionTimeout() },
                async { second.runtime.onElectionTimeout() },
            )
            elections.forEach { it.await() }
        }
        assertIs<CandidateNode>(first.runtime.currentNode)
        assertIs<CandidateNode>(second.runtime.currentNode)
        assertEquals(1L, first.store.load().currentTerm)
        assertEquals(1L, second.store.load().currentTerm)
    }

    @Test
    fun delayedVoteToOnePeerDoesNotBlockHigherTermVoteFromAnotherPeer() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        val election = launch { fixture.runtime.onElectionTimeout() }
        val pending = List(2) { transport.votes.receive() }
        withTimeout(1.seconds) {
            assertTrue(fixture.runtime.handleRequestVote(vote(term = 2).copy(candidateId = "node-3")).voteGranted)
        }
        pending.forEach { it.response.complete(RequestVoteResponse(1, true)) }
        election.join()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(2L, fixture.store.load().currentTerm)
        assertEquals("node-3", fixture.store.load().votedFor)
    }

    @Test
    fun voteFromPreviousElectionCannotElectCandidateInNewTerm() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        val firstElection = launch { fixture.runtime.onElectionTimeout() }
        val first = transport.votes.receive()
        val secondElection = launch { fixture.runtime.onElectionTimeout() }
        runCurrent()
        assertTrue(transport.votes.tryReceive().isFailure)
        assertEquals(1L, first.request.term)
        assertEquals(2L, fixture.store.load().currentTerm)
        // Even a response carrying the new term cannot count toward a newer election.
        first.response.complete(RequestVoteResponse(2, true))
        firstElection.join()
        assertIs<CandidateNode>(fixture.runtime.currentNode)
        val second = transport.votes.receive()
        assertEquals(2L, second.request.term)
        second.response.complete(RequestVoteResponse(2, true))
        secondElection.join()
        assertIs<LeaderNode>(fixture.runtime.currentNode)
    }

    @Test
    fun voteReceivedAfterSameTermAppendEntriesCannotRestoreCandidateRole() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        val election = launch { fixture.runtime.onElectionTimeout() }
        val pending = transport.votes.receive()
        withTimeout(1.seconds) {
            assertTrue(fixture.runtime.handleAppendEntries(heartbeat(term = 1)).success)
        }
        pending.response.complete(RequestVoteResponse(1, true))
        election.join()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
    }

    @Test
    fun higherTermVoteResponsePersistsTermAndResetsElectionTimer() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        val election = launch { fixture.runtime.onElectionTimeout() }
        val pending = transport.votes.receive()
        pending.response.complete(RequestVoteResponse(4, false))
        election.join()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(4L, fixture.store.load().currentTerm)
        assertEquals(null, fixture.store.load().votedFor)
        assertEquals(RaftTimeoutEvent.Election, fixture.timerEvents.last())
    }

    @Test
    fun delayedAppendDoesNotBlockIncomingAppendAndCannotCommitAfterDemotion() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val append = async { fixture.runtime.appendCommand(command(1)) }
        val pending = transport.appends.receive()
        withTimeout(1.seconds) {
            assertTrue(fixture.runtime.handleAppendEntries(heartbeat(term = 1)).success)
        }
        pending.response.complete(AppendEntriesResponse(1, true))
        assertEquals(command(1), append.await().command)
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(0L, fixture.state.commitIndex)
        assertEquals(emptyList(), fixture.applied)
    }

    @Test
    fun failedAppendRetriesBeforeQueuedReplicationWithoutRollingBackProgress() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val first = async { fixture.runtime.appendCommand(command(1)) }
        val older = transport.appends.receive()
        val second = async { fixture.runtime.appendCommand(command(2)) }
        runCurrent()
        assertTrue(transport.appends.tryReceive().isFailure)
        older.response.complete(AppendEntriesResponse(1, false))
        val retry = transport.appends.receive()
        assertEquals(listOf(command(1), command(2)), retry.request.entries.map { it.command })
        retry.response.complete(AppendEntriesResponse(1, true))
        val queued = transport.appends.receive()
        assertEquals(2L, queued.request.prevLogIndex)
        queued.response.complete(AppendEntriesResponse(1, true))
        second.await()
        first.await()
        val progress = assertIs<LeaderNode>(fixture.runtime.currentNode).peerProgress.getValue("node-2")
        assertEquals(3L, progress.nextIndex)
        assertEquals(2L, progress.matchIndex)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
        assertTrue(transport.appends.tryReceive().isFailure)
        assertEquals(1, transport.maximumInFlight.getValue("node-2"))
    }

    @Test
    fun lowerTermAppendResponseCannotAdvanceCommitOrTriggerRetry() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val append = async { fixture.runtime.appendCommand(command(1)) }
        val pending = transport.appends.receive()
        pending.response.complete(AppendEntriesResponse(0, true))
        append.await()
        assertEquals(0L, fixture.state.commitIndex)
        assertEquals(0L, assertIs<LeaderNode>(fixture.runtime.currentNode).peerProgress.getValue("node-2").matchIndex)
        assertTrue(transport.appends.tryReceive().isFailure)
    }

    @Test
    fun staleHigherTermAppendResponseStillDemotesLeader() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        elect(fixture.runtime, transport)
        val first = async { fixture.runtime.appendCommand(command(1)) }
        val pending = List(2) { transport.appends.receive() }.associateBy { it.peerId }
        pending.getValue("node-3").response.complete(AppendEntriesResponse(1, true))
        runCurrent()
        val second = async { fixture.runtime.appendCommand(command(2)) }
        val newer = transport.appends.receive()
        assertEquals("node-3", newer.peerId)
        newer.response.complete(AppendEntriesResponse(1, true))
        runCurrent()
        assertEquals(2L, fixture.state.commitIndex)
        pending.getValue("node-2").response.complete(AppendEntriesResponse(3, false))
        first.await()
        second.await()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(3L, fixture.store.load().currentTerm)
        assertEquals(null, fixture.store.load().votedFor)
        assertEquals(RaftTimeoutEvent.Election, fixture.timerEvents.last())
        assertEquals(2L, fixture.state.commitIndex)
    }

    @Test
    fun previousLeaderAppendCannotUpdateProgressAfterReelection() = runTest {
        previousLeaderAppendAfterReelection(success = true)
    }

    @Test
    fun previousLeaderAppendFailureCannotRollBackProgressAfterReelection() = runTest {
        previousLeaderAppendAfterReelection(success = false)
    }

    private suspend fun CoroutineScope.previousLeaderAppendAfterReelection(success: Boolean) {
        val transport = ControlledTransport()
        val fixture = fixture(transport, peerIds = setOf("node-2", "node-3"))
        elect(fixture.runtime, transport)
        val append = async { fixture.runtime.appendCommand(command(1)) }
        val pending = List(2) { transport.appends.receive() }.associateBy { it.peerId }
        pending.getValue("node-3").response.complete(AppendEntriesResponse(0, true))
        fixture.runtime.handleAppendEntries(heartbeat(term = 1))
        val election = launch { fixture.runtime.onElectionTimeout() }
        val vote = transport.votes.receive()
        assertEquals("node-3", vote.peerId)
        vote.response.complete(RequestVoteResponse(2, true))
        // Only the test scheduler is used here, so yield until the election applies its response.
        yield()
        assertEquals(2L, fixture.store.load().currentTerm)
        val leader = assertIs<LeaderNode>(fixture.runtime.currentNode)
        val progress = leader.peerProgress.getValue("node-2")
        pending.getValue("node-2").response.complete(AppendEntriesResponse(2, success))
        append.await()
        election.join()
        assertEquals(progress, leader.peerProgress.getValue("node-2"))
        assertEquals(0L, fixture.state.commitIndex)
    }

    private suspend fun elect(runtime: RaftRuntime, transport: ControlledTransport) {
        transport.automaticVotes = true
        try {
            runtime.onElectionTimeout()
        } finally {
            transport.automaticVotes = false
        }
        assertIs<LeaderNode>(runtime.currentNode)
    }

    private suspend fun fixture(
        transport: RaftTransport,
        nodeId: String = "node-1",
        peerIds: Set<String> = setOf("node-2"),
    ): Fixture {
        val store = MemoryRaftPersistentStateStore()
        val state = RaftVolatileState()
        val applied = mutableListOf<RaftCommand>()
        val events = mutableListOf<RaftTimeoutEvent>()
        val node = RaftNodeBuilder(
            nodeId, peerIds, store, state, MemoryRaftLog(),
            object : RaftStateMachine {
                override suspend fun apply(command: RaftCommand) { applied.add(command) }
            },
        ) { events.add(it) }.build()
        return Fixture(RaftRuntime(node, transport), store, state, applied, events)
    }

    private data class Fixture(
        val runtime: RaftRuntime,
        val store: MemoryRaftPersistentStateStore,
        val state: RaftVolatileState,
        val applied: List<RaftCommand>,
        val timerEvents: List<RaftTimeoutEvent>,
    )

    private class ControlledTransport : RaftTransport {
        val votes = Channel<PendingVote>(Channel.UNLIMITED)
        val appends = Channel<PendingAppend>(Channel.UNLIMITED)
        var automaticVotes = false
        val inFlight = mutableMapOf<String, Int>()
        val maximumInFlight = mutableMapOf<String, Int>()

        override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse = rpc(peerId) {
            if (automaticVotes) return@rpc RequestVoteResponse(request.term, true)
            val pending = PendingVote(peerId, request)
            votes.send(pending)
            pending.response.await()
        }

        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse = rpc(peerId) {
            val pending = PendingAppend(peerId, request)
            appends.send(pending)
            pending.response.await()
        }

        private suspend fun <T> rpc(peerId: String, block: suspend () -> T): T {
            val active = inFlight.getOrElse(peerId) { 0 } + 1
            inFlight[peerId] = active
            maximumInFlight[peerId] = maxOf(maximumInFlight.getOrElse(peerId) { 0 }, active)
            try {
                return block()
            } finally {
                inFlight[peerId] = inFlight.getValue(peerId) - 1
            }
        }
    }

    private data class PendingVote(
        val peerId: String,
        val request: RequestVoteRequest,
        val response: CompletableDeferred<RequestVoteResponse> = CompletableDeferred(),
    )

    private data class PendingAppend(
        val peerId: String,
        val request: AppendEntriesRequest,
        val response: CompletableDeferred<AppendEntriesResponse> = CompletableDeferred(),
    )

    private fun command(value: Int): RaftCommand = RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf(10))
    private fun vote(term: Long) = RequestVoteRequest(term, "node-2", 0, 0)
    private fun heartbeat(term: Long) = AppendEntriesRequest(term, "node-2", 0, 0, emptyList(), 0)
}
