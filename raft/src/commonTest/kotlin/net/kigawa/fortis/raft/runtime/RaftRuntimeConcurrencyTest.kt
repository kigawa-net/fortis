package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
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

class RaftRuntimeConcurrencyTest {
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
        val pending = transport.votes.receive()
        withTimeout(1.seconds) {
            assertTrue(fixture.runtime.handleRequestVote(vote(term = 2).copy(candidateId = "node-3")).voteGranted)
        }
        pending.response.complete(RequestVoteResponse(1, true))
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
        val second = transport.votes.receive()
        assertEquals(1L, first.request.term)
        assertEquals(2L, second.request.term)
        // Even a response carrying the new term cannot count toward a newer election.
        first.response.complete(RequestVoteResponse(2, true))
        firstElection.join()
        assertIs<CandidateNode>(fixture.runtime.currentNode)
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
    fun staleAppendFailureCannotRollBackProgressFromNewerSuccess() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val first = async { fixture.runtime.appendCommand(command(1)) }
        val older = transport.appends.receive()
        val second = async { fixture.runtime.appendCommand(command(2)) }
        val newer = transport.appends.receive()
        newer.response.complete(AppendEntriesResponse(1, true))
        second.await()
        older.response.complete(AppendEntriesResponse(1, false))
        first.await()
        val progress = assertIs<LeaderNode>(fixture.runtime.currentNode).peerProgress.getValue("node-2")
        assertEquals(3L, progress.nextIndex)
        assertEquals(2L, progress.matchIndex)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
        assertTrue(transport.appends.tryReceive().isFailure)
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
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val first = async { fixture.runtime.appendCommand(command(1)) }
        val older = transport.appends.receive()
        val second = async { fixture.runtime.appendCommand(command(2)) }
        val newer = transport.appends.receive()
        newer.response.complete(AppendEntriesResponse(1, true))
        second.await()
        older.response.complete(AppendEntriesResponse(3, false))
        first.await()
        assertIs<FollowerNode>(fixture.runtime.currentNode)
        assertEquals(3L, fixture.store.load().currentTerm)
        assertEquals(null, fixture.store.load().votedFor)
        assertEquals(RaftTimeoutEvent.Election, fixture.timerEvents.last())
        assertEquals(2L, fixture.state.commitIndex)
    }

    @Test
    fun previousLeaderAppendCannotUpdateProgressAfterReelection() = runTest {
        val transport = ControlledTransport()
        val fixture = fixture(transport)
        elect(fixture.runtime, transport)
        val append = async { fixture.runtime.appendCommand(command(1)) }
        val pending = transport.appends.receive()
        fixture.runtime.handleAppendEntries(heartbeat(term = 1))
        elect(fixture.runtime, transport)
        assertEquals(2L, fixture.store.load().currentTerm)
        val leader = assertIs<LeaderNode>(fixture.runtime.currentNode)
        val progress = leader.peerProgress.getValue("node-2")
        pending.response.complete(AppendEntriesResponse(2, true))
        append.await()
        assertEquals(progress, leader.peerProgress.getValue("node-2"))
        assertEquals(0L, fixture.state.commitIndex)
    }

    private suspend fun CoroutineScope.elect(runtime: RaftRuntime, transport: ControlledTransport) {
        val election = launch { runtime.onElectionTimeout() }
        val pending = transport.votes.receive()
        pending.response.complete(RequestVoteResponse(pending.request.term, true))
        election.join()
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

        override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse {
            val pending = PendingVote(request)
            votes.send(pending)
            return pending.response.await()
        }

        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse {
            val pending = PendingAppend(request)
            appends.send(pending)
            return pending.response.await()
        }
    }

    private data class PendingVote(
        val request: RequestVoteRequest,
        val response: CompletableDeferred<RequestVoteResponse> = CompletableDeferred(),
    )

    private data class PendingAppend(
        val request: AppendEntriesRequest,
        val response: CompletableDeferred<AppendEntriesResponse> = CompletableDeferred(),
    )

    private fun command(value: Int): RaftCommand = RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf(10))
    private fun vote(term: Long) = RequestVoteRequest(term, "node-2", 0, 0)
    private fun heartbeat(term: Long) = AppendEntriesRequest(term, "node-2", 0, 0, emptyList(), 0)
}
