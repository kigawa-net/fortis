package net.kigawa.fortis.raft.logging

import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.test.TestScope
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull

class RaftRuntimeLoggingTest {
    @Test
    fun singleNodeElectionIsRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger, emptySet())

        runtime.onElectionTimeout()

        assertIs<LeaderNode>(runtime.currentNode)
        assertEquals(
            listOf(RaftLogEvents.ELECTION_STARTED, RaftLogEvents.LEADER_ELECTED),
            logger.entries.map { it.event },
        )
        assertEquals(listOf("1", "1"), logger.entries.map { it.fields["term"] })
    }

    @Test
    fun proposalAppendIsRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)
        elect(runtime)
        logger.entries.clear()

        try {
            val entry = runtime.propose(RaftCommand.Put(byteArrayOf(1), byteArrayOf(10)))

            val appended = logger.entries.single { it.event == RaftLogEvents.COMMAND_APPENDED }
            assertEquals(LogLevel.DEBUG, appended.level)
            assertEquals(RaftLogEvents.COMPONENT_RUNTIME, appended.component)
            assertEquals(mapOf("index" to entry.index.toString(), "term" to entry.term.toString()), appended.fields)
        } finally {
            runtime.stop()
        }
    }

    @Test
    fun voteRequestDemotionIsRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)
        elect(runtime)
        logger.entries.clear()

        runtime.handleRequestVote(RequestVoteRequest(2, "peer-1", 1, 1))

        val demoted = logger.entries.single { it.event == RaftLogEvents.LEADER_DEMOTED }
        assertEquals(LogLevel.WARN, demoted.level)
        assertEquals("2", demoted.fields["term"])
    }

    @Test
    fun snapshotDemotionIsRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)
        elect(runtime)
        logger.entries.clear()

        runtime.handleInstallSnapshot(
            InstallSnapshotRequest(2, "peer-1", SnapshotMetadata(1, 1), byteArrayOf(1)),
        )

        val demoted = logger.entries.single { it.event == RaftLogEvents.LEADER_DEMOTED }
        assertEquals(LogLevel.WARN, demoted.level)
        assertEquals("2", demoted.fields["term"])
    }

    @Test
    fun electionStartedAndLeaderElectedAreRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)

        runtime.onElectionTimeout()

        val started = logger.entries.firstOrNull { it.event == RaftLogEvents.ELECTION_STARTED }
        assertNotNull(started)
        assertEquals(LogLevel.INFO, started.level)
        assertEquals(RaftLogEvents.COMPONENT_RUNTIME, started.component)
        assertEquals("1", started.fields["term"])

        val elected = logger.entries.firstOrNull { it.event == RaftLogEvents.LEADER_ELECTED }
        assertNotNull(elected)
        assertEquals(LogLevel.INFO, elected.level)
        assertEquals("1", elected.fields["term"])
    }

    @Test
    fun heartbeatAndCommandAppendedAreRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)
        elect(runtime)
        logger.entries.clear()

        runtime.onHeartbeatTimeout()
        runtime.appendCommand(RaftCommand.Put(byteArrayOf(1), byteArrayOf(10)))

        val heartbeat = logger.entries.firstOrNull { it.event == RaftLogEvents.HEARTBEAT_SENT }
        assertNotNull(heartbeat)
        assertEquals(LogLevel.DEBUG, heartbeat.level)
        assertEquals(RaftLogEvents.COMPONENT_RUNTIME, heartbeat.component)
        assertEquals("1", heartbeat.fields["term"])

        val appended = logger.entries.firstOrNull { it.event == RaftLogEvents.COMMAND_APPENDED }
        assertNotNull(appended)
        assertEquals(LogLevel.DEBUG, appended.level)
        // Leader election appends a no-op entry at index 1 first.
        assertEquals("2", appended.fields["index"])
    }

    @Test
    fun installSnapshotReceivedIsRecorded() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)

        runtime.handleInstallSnapshot(
            InstallSnapshotRequest(
                term = 1,
                leaderId = "leader",
                metadata = SnapshotMetadata(1, 1),
                data = byteArrayOf(1, 2, 3),
            ),
        )

        val received = logger.entries.firstOrNull { it.event == RaftLogEvents.INSTALL_SNAPSHOT_RECEIVED }
        assertNotNull(received)
        assertEquals(LogLevel.INFO, received.level)
        assertEquals(RaftLogEvents.COMPONENT_RUNTIME, received.component)
        assertEquals("1", received.fields["term"])
        assertEquals("leader", received.fields["leaderId"])
    }

    @Test
    fun leaderDemotedIsRecordedOnHigherTerm() = runTest {
        val logger = RecordingLogger()
        val runtime = fixture(logger)
        elect(runtime)
        val leader = runtime.currentNode as LeaderNode
        leader.appendCommand(RaftCommand.Put(byteArrayOf(1), byteArrayOf(10)))

        runtime.handleAppendEntries(
            AppendEntriesRequest(
                term = 2,
                leaderId = "peer-1",
                prevLogIndex = 1,
                prevLogTerm = 1,
                entries = emptyList(),
                leaderCommit = 1,
            ),
        )

        val demoted = logger.entries.firstOrNull { it.event == RaftLogEvents.LEADER_DEMOTED }
        assertNotNull(demoted)
        assertEquals(LogLevel.WARN, demoted.level)
        assertEquals("2", demoted.fields["term"])
    }

    private suspend fun elect(runtime: RaftRuntime) {
        runtime.onElectionTimeout()
        assertIs<LeaderNode>(runtime.currentNode)
    }

    private suspend fun TestScope.fixture(logger: RaftLogger, peerIds: Set<String> = setOf("peer-1")): RaftRuntime {
        val store = MemoryRaftPersistentStateStore(RaftPersistentState())
        val node = RaftNodeBuilder(
            nodeId = "self",
            peerIds = peerIds,
            persistentStateStore = store,
            volatileState = RaftVolatileState(),
            log = MemoryRaftLog(),
            stateMachine = object : RaftStateMachine {
                override suspend fun apply(command: RaftCommand) = Unit
            },
            timer = RaftTimer.None,
        ).build()
        return RaftRuntime(node, RecordingTransport(), backgroundScope.coroutineContext, logger = logger)
    }

    private class RecordingTransport : RaftTransport {
        override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse =
            RequestVoteResponse(request.term, true)

        override suspend fun appendEntries(peerId: String, request: AppendEntriesRequest): AppendEntriesResponse =
            AppendEntriesResponse(request.term, true)

        override suspend fun installSnapshot(
            peerId: String,
            request: InstallSnapshotRequest,
        ): InstallSnapshotResponse = InstallSnapshotResponse(request.term)
    }
}
