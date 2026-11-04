package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

// InstallSnapshot 受信処理のふるまいを確認する
class InstallSnapshotHandlerTest {
    @Test
    fun metadataRejectsInvalidBounds() {
        assertFailsWith<IllegalArgumentException> { SnapshotMetadata(0, 1) }
        assertFailsWith<IllegalArgumentException> { SnapshotMetadata(-1, 1) }
        assertFailsWith<IllegalArgumentException> { SnapshotMetadata(1, -1) }
    }

    @Test
    fun freshSnapshotAppliesAndResetsTimer() = runTest {
        val fixture = Fixture(term = 2)
        fixture.appendEntries(1L..3L, term = 2)

        val response = fixture.handler.handle(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(3, 3),
                data = byteArrayOf(9),
            ),
        )

        assertEquals(3, response.term)
        assertEquals(3, fixture.persistentState.currentTerm)
        assertNull(fixture.persistentState.votedFor)
        assertEquals(1, fixture.timerResets)
        // 境界エントリとターム不一致の新規スナップショットのためログ全体が圧縮される
        assertEquals(0, fixture.log.lastIndex())
        assertEquals(3, fixture.volatileState.commitIndex)
        assertEquals(3, fixture.volatileState.lastApplied)
        assertEquals(1, fixture.applied.size)
        val (appliedMetadata, appliedData) = fixture.applied.single()
        assertEquals(3L, appliedMetadata.lastIncludedIndex)
        assertTrue(appliedData.contentEquals(byteArrayOf(9)))
    }

    @Test
    fun staleSnapshotIsIgnoredWithCurrentTerm() = runTest {
        val fixture = Fixture(term = 4, initialSnapshotIndex = 3)
        fixture.appendEntries(1L..4L, term = 4)

        val response = fixture.handler.handle(
            InstallSnapshotRequest(
                term = 4,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(2, 4),
                data = byteArrayOf(1),
            ),
        )

        assertEquals(4, response.term)
        // 生存リーダーの証明としてタイマーはリセットされるがログと適用状態は変わらない
        assertEquals(1, fixture.timerResets)
        assertEquals(4, fixture.log.lastIndex())
        assertEquals(0, fixture.volatileState.commitIndex)
        assertTrue(fixture.applied.isEmpty())
    }

    @Test
    fun lowerTermIsRejectedWithoutSideEffects() = runTest {
        val fixture = Fixture(term = 5)
        fixture.appendEntries(1L..2L, term = 5)

        val response = fixture.handler.handle(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(2, 5),
                data = byteArrayOf(1),
            ),
        )

        assertEquals(5, response.term)
        assertEquals(0, fixture.timerResets)
        assertEquals(2, fixture.log.lastIndex())
        assertTrue(fixture.applied.isEmpty())
    }

    @Test
    fun matchingBoundaryKeepsSuffixEntries() = runTest {
        val fixture = Fixture(term = 2)
        fixture.appendEntries(1L..4L, term = 2)

        fixture.handler.handle(
            InstallSnapshotRequest(
                term = 2,
                leaderId = "leader-1",
                metadata = SnapshotMetadata(2, 2),
                data = byteArrayOf(7),
            ),
        )

        // 境界一致のため後続エントリは保持される（境界以前の物理圧縮は将来課題）
        assertEquals(4, fixture.log.lastIndex())
        assertEquals(2, fixture.volatileState.commitIndex)
        assertEquals(2, fixture.volatileState.lastApplied)
    }

    private class Fixture(
        term: Long,
        initialSnapshotIndex: Long = 0,
    ) {
        val persistentState = RaftPersistentState(term, "node-9")
        val store = MemoryRaftPersistentStateStore(persistentState)
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        var timerResets = 0
        val applied = mutableListOf<Pair<SnapshotMetadata, ByteArray>>()
        val handler = InstallSnapshotHandler(
            persistentState = persistentState,
            persistentStateStore = store,
            volatileState = volatileState,
            log = log,
            timer = RaftTimer { event ->
                if (event == RaftTimeoutEvent.Election) timerResets++
            },
            snapshotApplier = RaftSnapshotApplier { metadata, data ->
                applied.add(metadata to data.copyOf())
            },
            initialLastIncludedIndex = initialSnapshotIndex,
        )

        suspend fun appendEntries(
            indexes: LongRange,
            term: Long,
        ) {
            for (index in indexes) {
                log.append(
                    RaftLogEntry(
                        index = index,
                        term = term,
                        command = RaftCommand.Put(byteArrayOf(index.toByte()), byteArrayOf(1)),
                    ),
                )
            }
        }
    }
}
