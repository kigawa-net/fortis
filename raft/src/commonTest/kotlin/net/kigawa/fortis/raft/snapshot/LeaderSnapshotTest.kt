package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

// リーダー側スナップショット判断・生成・応答反映を確認する
class LeaderSnapshotTest {
    @Test
    fun needsSnapshotIsFalseWhenUnset() = runTest {
        val fixture = Fixture(nextIndex = 1)

        assertFalse(fixture.leader.needsSnapshot("node-2"))
    }

    @Test
    fun needsSnapshotFollowsPeerProgress() = runTest {
        val behind = Fixture(nextIndex = 2)
        behind.leader.updateSnapshotMetadata(SnapshotMetadata(3, 1))
        assertTrue(behind.leader.needsSnapshot("node-2"))

        val boundary = Fixture(nextIndex = 3)
        boundary.leader.updateSnapshotMetadata(SnapshotMetadata(3, 1))
        assertTrue(boundary.leader.needsSnapshot("node-2"))

        val ahead = Fixture(nextIndex = 4)
        ahead.leader.updateSnapshotMetadata(SnapshotMetadata(3, 1))
        assertFalse(ahead.leader.needsSnapshot("node-2"))
    }

    @Test
    fun createInstallSnapshotBuildsRequest() = runTest {
        val fixture = Fixture(nextIndex = 2)
        val metadata = SnapshotMetadata(3, 1)
        fixture.leader.updateSnapshotMetadata(metadata)

        val request = fixture.leader.createInstallSnapshot("node-2", byteArrayOf(9, 8))

        assertEquals(1, request.term)
        assertEquals("node-1", request.leaderId)
        assertEquals(metadata, request.metadata)
        assertTrue(request.data.contentEquals(byteArrayOf(9, 8)))
    }

    @Test
    fun createInstallSnapshotFailsWithoutMetadata() = runTest {
        val fixture = Fixture(nextIndex = 2)

        assertFailsWith<IllegalStateException> {
            fixture.leader.createInstallSnapshot("node-2", byteArrayOf(1))
        }
    }

    @Test
    fun handleInstallSnapshotResponseAdvancesProgress() = runTest {
        val fixture = Fixture(nextIndex = 2)
        val request = InstallSnapshotRequest(
            term = 1,
            leaderId = "node-1",
            metadata = SnapshotMetadata(3, 1),
            data = byteArrayOf(7),
        )

        val next = fixture.leader.handleInstallSnapshotResponse(
            "node-2",
            request,
            InstallSnapshotResponse(term = 1),
        )

        assertIs<LeaderNode>(next)
        assertEquals(RaftPeerProgress(nextIndex = 4, matchIndex = 3), fixture.leader.peerProgress.getValue("node-2"))
    }

    @Test
    fun handleInstallSnapshotResponseStepsDownOnHigherTerm() = runTest {
        val fixture = Fixture(nextIndex = 2)
        val request = InstallSnapshotRequest(
            term = 1,
            leaderId = "node-1",
            metadata = SnapshotMetadata(3, 1),
            data = byteArrayOf(7),
        )

        val next = fixture.leader.handleInstallSnapshotResponse(
            "node-2",
            request,
            InstallSnapshotResponse(term = 2),
        )

        assertIs<FollowerNode>(next)
        assertEquals(2, fixture.persistentState.currentTerm)
        assertEquals(RaftPeerProgress(nextIndex = 2), fixture.leader.peerProgress.getValue("node-2"))
    }

    private class Fixture(nextIndex: Long) {
        val persistentState = RaftPersistentState(currentTerm = 1)
        val leader = LeaderNode(
            nodeId = "node-1",
            peerIds = setOf("node-2"),
            persistentState = persistentState,
            persistentStateStore = MemoryRaftPersistentStateStore(persistentState),
            volatileState = RaftVolatileState(),
            log = MemoryRaftLog(),
            stateMachine = RecordingStateMachine(),
            timer = RaftTimer.None,
            peerProgress = mapOf("node-2" to RaftPeerProgress(nextIndex = nextIndex)),
        )
    }

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
