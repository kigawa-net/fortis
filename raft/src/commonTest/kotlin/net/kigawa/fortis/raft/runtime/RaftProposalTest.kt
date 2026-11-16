package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.*
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.RaftPeerUnavailableException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class RaftProposalTest {
    @Test
    fun majorityCommitAndApplyReturnsReceiptWithoutWaitingForSlowPeer() = runTest {
        val transport = Transport()
        val slow = CompletableDeferred<Unit>()
        transport.beforeAppend = { peer -> if (peer == "node-2") slow.await() }
        val fixture = fixture(transport)
        val proposal = async { fixture.runtime.propose(command(1)) }
        runCurrent()
        assertTrue(proposal.isCompleted)
        val receipt = proposal.await()
        assertEquals(2L, receipt.index)
        assertEquals(1L, receipt.term)
        assertEquals(command(1), receipt.command)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(2L, fixture.state.lastApplied)
        assertEquals(listOf(command(1)), fixture.applied)
        slow.complete(Unit)
        fixture.runtime.stop()
    }

    @Test
    fun unavailableMajorityCannotReturnSuccessAndTimesOut() = runTest {
        val transport = Transport().also { it.unavailable = true }
        val fixture = fixture(transport)
        val proposal = async {
            assertFailsWith<TimeoutCancellationException> {
                fixture.runtime.propose(command(1), 100.milliseconds)
            }
        }
        runCurrent()
        assertTrue(proposal.isActive)
        assertEquals(2L, fixture.log.lastIndex())
        assertEquals(0L, fixture.state.commitIndex)
        advanceTimeBy(100.milliseconds)
        runCurrent()
        proposal.await()
        assertEquals(emptyList(), fixture.applied)
        fixture.runtime.stop()
    }

    @Test
    fun commitIsNotAcknowledgedUntilLocalApplyCompletes() = runTest {
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        val fixture = fixture(beforeApply = { entered.complete(Unit); release.await() })
        val proposal = async { fixture.runtime.propose(command(1)) }
        runCurrent()
        assertTrue(entered.isCompleted)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        assertTrue(proposal.isActive)
        release.complete(Unit)
        runCurrent()
        assertEquals(2L, proposal.await().index)
        assertEquals(2L, fixture.state.lastApplied)
        fixture.runtime.stop()
    }

    @Test
    fun timeoutDuringSingleNodeApplyEndsWaitButDoesNotCancelAcceptedEntry() = runTest {
        val release = CompletableDeferred<Unit>()
        val fixture = fixture(peerIds = emptySet(), beforeApply = { release.await() })
        val proposal = async {
            assertFailsWith<TimeoutCancellationException> {
                fixture.runtime.propose(command(1), 100.milliseconds)
            }
        }
        runCurrent()
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        advanceTimeBy(100.milliseconds)
        runCurrent()
        assertTrue(proposal.isCompleted)
        proposal.await()
        release.complete(Unit)
        runCurrent()
        assertEquals(listOf(command(1)), fixture.applied)
        assertEquals(2L, fixture.state.lastApplied)
        assertEquals(3L, fixture.runtime.propose(command(2)).index)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
        fixture.runtime.stop()
    }

    @Test
    fun incomingLeaderFailsPendingProposalWithIndexAndTerm() = runTest {
        val transport = Transport().also { it.unavailable = true }
        val fixture = fixture(transport)
        val proposal = async {
            assertFailsWith<RaftProposalLeadershipLostException> { fixture.runtime.propose(command(1)) }
        }
        runCurrent()
        fixture.runtime.handleAppendEntries(AppendEntriesRequest(1, "node-2", 0, 0, emptyList(), 0))
        val failure = proposal.await()
        assertEquals(2L, failure.index)
        assertEquals(1L, failure.term)
        assertEquals(0L, fixture.state.commitIndex)
        fixture.runtime.stop()
    }

    @Test
    fun higherTermResponseFailsPendingProposal() = runTest {
        val transport = Transport().also { it.responseTerm = 2 }
        val fixture = fixture(transport)
        assertFailsWith<RaftProposalLeadershipLostException> { fixture.runtime.propose(command(1)) }
        assertEquals(2L, fixture.runtime.currentNode.persistentState.currentTerm)
        assertEquals(0L, fixture.state.commitIndex)
        fixture.runtime.stop()
    }

    @Test
    fun cancellationAfterAppendDoesNotRollBackLogAndHeartbeatCanCommitIt() = runTest {
        val transport = Transport().also { it.unavailable = true }
        val fixture = fixture(transport)
        val proposal = async { fixture.runtime.propose(command(1)) }
        runCurrent()
        assertEquals(2L, fixture.log.lastIndex())
        proposal.cancelAndJoin()
        assertFailsWith<CancellationException> { proposal.await() }
        transport.unavailable = false
        fixture.runtime.onHeartbeatTimeout()
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(listOf(command(1)), fixture.applied)
        fixture.runtime.stop()
    }

    @Test
    fun cancellationBeforeSubmissionDoesNotAppendCommand() = runTest {
        val fixture = fixture()
        val proposal = async(start = CoroutineStart.UNDISPATCHED) { fixture.runtime.propose(command(1)) }
        proposal.cancelAndJoin()
        runCurrent()
        assertEquals(1L, fixture.log.lastIndex())
        assertEquals(2L, fixture.runtime.propose(command(2)).index)
        assertEquals(listOf(command(2)), fixture.applied)
        fixture.runtime.stop()
    }

    @Test
    fun laterReplicationCompletesEachPendingProposalAtItsOwnIndex() = runTest {
        val transport = Transport().also { it.unavailable = true }
        val fixture = fixture(transport)
        val first = async { fixture.runtime.propose(command(1)) }
        val second = async { fixture.runtime.propose(command(2)) }
        runCurrent()
        assertEquals(3L, fixture.log.lastIndex())
        assertTrue(first.isActive && second.isActive)
        transport.unavailable = false
        fixture.runtime.replicate()
        assertEquals(2L, first.await().index)
        assertEquals(3L, second.await().index)
        assertEquals(listOf(command(1), command(2)), fixture.applied)
        fixture.runtime.stop()
    }

    @Test
    fun failedApplyNeverReturnsSuccess() = runTest {
        val fixture = fixture(beforeApply = { error("apply failed") })
        val failure = assertFailsWith<IllegalStateException> { fixture.runtime.propose(command(1)) }
        assertEquals("apply failed", failure.message)
        assertEquals(2L, fixture.state.commitIndex)
        assertEquals(1L, fixture.state.lastApplied)
        fixture.runtime.stop()
    }

    @Test
    fun followerRejectsProposalWithoutAppending() = runTest {
        val fixture = fixture(elect = false)
        assertFailsWith<RaftProposalNotLeaderException> { fixture.runtime.propose(command(1)) }
        assertEquals(0L, fixture.log.lastIndex())
        fixture.runtime.stop()
    }

    @Test
    fun stoppingRuntimeFailsPendingAndNewProposals() = runTest {
        val transport = Transport().also { it.unavailable = true }
        val fixture = fixture(transport)
        val proposal = async {
            assertFailsWith<RaftProposalStoppedException> { fixture.runtime.propose(command(1)) }
        }
        runCurrent()
        fixture.runtime.stop()
        proposal.await()
        assertFailsWith<RaftProposalStoppedException> { fixture.runtime.propose(command(2)) }
        assertEquals(2L, fixture.log.lastIndex())
    }

    private suspend fun TestScope.fixture(
        transport: Transport = Transport(),
        peerIds: Set<String> = setOf("node-2", "node-3"),
        beforeApply: suspend () -> Unit = {},
        elect: Boolean = true,
    ): Fixture {
        val state = RaftVolatileState()
        val log = MemoryRaftLog()
        val applied = mutableListOf<RaftCommand>()
        val node = RaftNodeBuilder(
            "node-1", peerIds, MemoryRaftPersistentStateStore(), state, log,
            object : RaftStateMachine {
                override suspend fun apply(command: RaftCommand) {
                    beforeApply()
                    applied.add(command)
                }
            },
        ).build()
        val runtime = RaftRuntime(node, transport, backgroundScope.coroutineContext)
        if (elect) {
            runtime.onElectionTimeout()
            assertIs<LeaderNode>(runtime.currentNode)
        }
        return Fixture(runtime, state, log, applied)
    }

    private data class Fixture(
        val runtime: RaftRuntime,
        val state: RaftVolatileState,
        val log: MemoryRaftLog,
        val applied: List<RaftCommand>,
    )

    private class Transport : RaftTransport {
        var unavailable = false
        var responseTerm: Long? = null
        var beforeAppend: suspend (String) -> Unit = {}

        override suspend fun requestVote(peerId: String, request: RequestVoteRequest) =
            RequestVoteResponse(request.term, true)

        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse {
            beforeAppend(peerId)
            if (unavailable) throw RaftPeerUnavailableException(peerId)
            val term = responseTerm ?: request.term
            return AppendEntriesResponse(term, term == request.term)
        }
    }

    private fun command(value: Int): RaftCommand = RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf(10))
}
