package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.CoroutineRaftTimer
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
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class RaftRuntimeTimerIntegrationTest {
    @Test
    fun timersElectOneLeaderAndSendHeartbeats() = runTest {
        val localTransport = LocalRaftTransport()
        val transport = RecordingTransport(localTransport)
        val runtimes = mapOf(
            "node-1" to runtime(
                "node-1",
                100.milliseconds,
                transport,
                localTransport,
            ),
            "node-2" to runtime(
                "node-2",
                200.milliseconds,
                transport,
                localTransport,
            ),
            "node-3" to runtime(
                "node-3",
                300.milliseconds,
                transport,
                localTransport,
            ),
        )
        runtimes.values.forEach { it.start() }

        advanceTimeBy(100.milliseconds)
        runCurrent()

        assertIs<LeaderNode>(runtimes.getValue("node-1").currentNode)
        assertIs<FollowerNode>(runtimes.getValue("node-2").currentNode)
        assertIs<FollowerNode>(runtimes.getValue("node-3").currentNode)

        transport.appendRequests.clear()
        advanceTimeBy(40.milliseconds)
        runCurrent()

        assertEquals(setOf("node-2", "node-3"), transport.appendRequests.keys)
        assertEquals(
            2,
            transport.appendRequests.values.sumOf { it.size },
        )

        runtimes.values.forEach { it.stop() }
        runCurrent()
    }

    private suspend fun TestScope.runtime(
        nodeId: String,
        electionTimeout: Duration,
        transport: RaftTransport,
        localTransport: LocalRaftTransport,
    ): RaftRuntime {
        lateinit var runtime: RaftRuntime
        val timer = CoroutineRaftTimer(
            scope = this,
            electionTimeoutProvider = {
                electionTimeout
            },
            heartbeatInterval = 40.milliseconds,
            onTimeout = { event ->
                when (event) {
                    RaftTimeoutEvent.Election -> runtime.onElectionTimeout()
                    RaftTimeoutEvent.Heartbeat -> runtime.onHeartbeatTimeout()
                }
            },
        )
        val peerIds = setOf("node-1", "node-2", "node-3") - nodeId
        val follower = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = peerIds,
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = RaftVolatileState(),
            log = MemoryRaftLog(),
            stateMachine = NoOpStateMachine(),
            timer = timer,
        ).build()
        runtime = RaftRuntime(follower, transport)
        localTransport.register(nodeId, runtime)
        return runtime
    }

    private class RecordingTransport(
        private val delegate: RaftTransport,
    ): RaftTransport {
        val appendRequests = mutableMapOf<String, MutableList<AppendEntriesRequest>>()

        override suspend fun requestVote(
            peerId: String,
            request: RequestVoteRequest,
        ): RequestVoteResponse = delegate.requestVote(peerId, request)

        override suspend fun appendEntries(
            peerId: String,
            request: AppendEntriesRequest,
        ): AppendEntriesResponse {
            appendRequests.getOrPut(peerId) { mutableListOf() }.add(request)
            return delegate.appendEntries(peerId, request)
        }
    }

    private class NoOpStateMachine: RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }
}
