package net.kigawa.fortis.raft.scan

import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.transport.LocalRaftTransport
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RevisionScanClientTest {
    private val prefix = byteArrayOf(0x70) // 'p'
    private val other = byteArrayOf(0x71) // 'q'

    @Test
    fun pagesCoverEveryLiveKeyOnceAtAnyPageSize() = runTest {
        val fixture = fixture()
        val expected = (0 until 50).map { key("p", it) }
        for (key in expected) fixture.runtime.propose(RaftCommand.Put(key, value(1)))
        fixture.runtime.propose(RaftCommand.Put(other, value(9)))

        for (pageSize in listOf(1, 7, 256)) {
            val keys = fixture.scanAll(pageSize)
            assertEquals(expected.map { it.toList() }, keys.map { it.toList() }, "pageSize=$pageSize")
        }
        fixture.runtime.stop()
    }

    @Test
    fun writesAfterFirstPageDoNotChangePinnedRevisionResults() = runTest {
        val fixture = fixture()
        for (i in 0 until 10) fixture.runtime.propose(RaftCommand.Put(key("p", i), value(i)))
        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 3)))
        val pinned = first.revision

        // 1 ページ目の後に更新・削除・追加を行う
        fixture.runtime.propose(RaftCommand.Put(key("p", 5), value(99)))
        fixture.runtime.propose(RaftCommand.Delete(key("p", 6)))
        fixture.runtime.propose(RaftCommand.Put(key("p", 50), value(50)))

        val rest = fixture.scanFrom(first, pageSize = 3)
        val all = first.entries + rest
        assertEquals((0 until 10).map { key("p", it).toList() }, all.map { it.key.toList() })
        assertTrue(rest.all { it.revision <= pinned })
        val fifth = all.single { it.key.contentEquals(key("p", 5)) }
        assertContentEquals(value(5), fifth.value)
        assertEquals(pinned, first.revision)
        fixture.runtime.stop()
    }

    @Test
    fun tombstoneHidesKeyAtAndAfterDeleteAndIsOmittedFromPages() = runTest {
        val fixture = fixture()
        fixture.runtime.propose(RaftCommand.Put(key("p", 1), value(1)))
        fixture.runtime.propose(RaftCommand.Put(key("p", 2), value(2)))
        fixture.runtime.propose(RaftCommand.Delete(key("p", 1)))

        val page = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 10)))

        assertEquals(listOf(key("p", 2).toList()), page.entries.map { it.key.toList() })
        assertNull(page.nextCursor)
        fixture.runtime.stop()
    }

    @Test
    fun partialPageNeverLooksCompleteAndLastPageHasNoCursor() = runTest {
        val fixture = fixture()
        for (i in 0 until 3) fixture.runtime.propose(RaftCommand.Put(key("p", i), value(i)))

        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 2)))
        assertEquals(2, first.entries.size)
        assertNotNull(first.nextCursor)
        val last = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 2, cursor = first.nextCursor)))
        assertEquals(1, last.entries.size)
        assertNull(last.nextCursor)
        fixture.runtime.stop()
    }

    @Test
    fun compactionAboveCursorRevisionFailsExplicitlyWithoutPartialPage() = runTest {
        val fixture = fixture()
        for (i in 0 until 4) fixture.runtime.propose(RaftCommand.Put(key("p", i), value(i)))
        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 2)))
        val cursor = assertNotNull(first.nextCursor)

        // 固定 revision より後まで compact されると、残りのページは返らない
        fixture.engine.compact(first.revision + 1)
        assertEquals(
            ScanResult.RevisionCompacted(first.revision + 1),
            fixture.client.scanAtRevision(request(pageSize = 2, cursor = cursor)),
        )
        assertEquals(
            ScanResult.RevisionCompacted(first.revision + 1),
            fixture.client.scanAtRevision(request(pageSize = 2, readRevision = first.revision)),
        )
        fixture.runtime.stop()
    }

    @Test
    fun compactionAtOrBelowPinnedRevisionKeepsRemainingPagesCorrect() = runTest {
        val fixture = fixture()
        for (i in 0 until 6) fixture.runtime.propose(RaftCommand.Put(key("p", i), value(i)))
        fixture.runtime.propose(RaftCommand.Put(key("p", 0), value(100)))
        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 2)))

        fixture.engine.compact(first.revision)
        val keys = first.entries.map { it.key } + fixture.scanFrom(first, pageSize = 2).map { it.key }

        assertEquals((0 until 6).map { key("p", it).toList() }, keys.map { it.toList() })
        fixture.runtime.stop()
    }

    @Test
    fun invalidCursorsAreRejected() = runTest {
        val fixture = fixture()
        for (i in 0 until 3) fixture.runtime.propose(RaftCommand.Put(key("p", i), value(i)))
        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(request(pageSize = 1)))
        val cursor = assertNotNull(first.nextCursor)
        val bytes = cursor.toBytes()

        val flipped = bytes.copyOf().also { it[10] = (it[10].toInt() xor 0x01).toByte() }
        assertIs<ScanResult.InvalidCursor>(fixture.client.scanAtRevision(request(1, cursor = OpaqueCursor(flipped))))
        assertIs<ScanResult.InvalidCursor>(
            fixture.client.scanAtRevision(request(1, cursor = OpaqueCursor(bytes.copyOf(bytes.size - 1)))),
        )
        assertIs<ScanResult.InvalidCursor>(fixture.client.scanAtRevision(request(1, cursor = OpaqueCursor(byteArrayOf(1, 2, 3)))))
        // 別 prefix への流用
        assertIs<ScanResult.InvalidCursor>(
            fixture.client.scanAtRevision(ScanRequest(other, cursor = cursor, pageSize = 1)),
        )
        // readRevision と cursor の revision の不一致
        assertIs<ScanResult.InvalidCursor>(
            fixture.client.scanAtRevision(ScanRequest(prefix, readRevision = first.revision + 1, cursor = cursor, pageSize = 1)),
        )
        fixture.runtime.stop()
    }

    @Test
    fun readRevisionAheadOfAppliedStateIsUnavailable() = runTest {
        val fixture = fixture()
        fixture.runtime.propose(RaftCommand.Put(key("p", 0), value(0)))
        val latest = fixture.machine.latestRevision()

        assertEquals(
            ScanResult.RevisionUnavailable(latest + 5, latest),
            fixture.client.scanAtRevision(request(pageSize = 1, readRevision = latest + 5)),
        )
        fixture.runtime.stop()
    }

    @Test
    fun followerCannotConfirmRevisionAndReturnsUnavailable() = runTest {
        val fixture = fixture(elect = false)

        assertEquals(
            ScanResult.Unavailable("NOT_LEADER"),
            fixture.client.scanAtRevision(request(pageSize = 1)),
        )
        fixture.runtime.stop()
    }

    @Test
    fun emptyKeyAndHighBytesFollowBytewiseOrderAcrossCursor() = runTest {
        val fixture = fixture()
        fixture.runtime.propose(RaftCommand.Put(byteArrayOf(), value(1)))
        fixture.runtime.propose(RaftCommand.Put(byteArrayOf(0x80.toByte()), value(2)))
        fixture.runtime.propose(RaftCommand.Put(byteArrayOf(0x01), value(3)))
        val empty = ByteArray(0)

        val first = assertIs<ScanResult.Page>(fixture.client.scanAtRevision(ScanRequest(empty, pageSize = 1)))
        val second = assertIs<ScanResult.Page>(
            fixture.client.scanAtRevision(ScanRequest(empty, cursor = first.nextCursor, pageSize = 1)),
        )
        val third = assertIs<ScanResult.Page>(
            fixture.client.scanAtRevision(ScanRequest(empty, cursor = second.nextCursor, pageSize = 1)),
        )

        assertEquals(listOf(listOf<Byte>()), first.entries.map { it.key.toList() })
        assertEquals(listOf(listOf<Byte>(0x01)), second.entries.map { it.key.toList() })
        assertEquals(listOf(listOf<Byte>(0x80.toByte())), third.entries.map { it.key.toList() })
        assertNull(third.nextCursor)
        fixture.runtime.stop()
    }

    @Test
    fun invalidRequestArgumentsAreRejected() {
        assertFailsWith<IllegalArgumentException> { ScanRequest(prefix, pageSize = 0) }
        assertFailsWith<IllegalArgumentException> { ScanRequest(prefix, pageSize = ScanRequest.MAX_PAGE_SIZE + 1) }
        assertFailsWith<IllegalArgumentException> { ScanRequest(ByteArray(ScanRequest.MAX_PREFIX_BYTES + 1), pageSize = 1) }
        assertFailsWith<IllegalArgumentException> { ScanRequest(prefix, readRevision = -1, pageSize = 1) }
    }

    private fun request(
        pageSize: Int,
        cursor: OpaqueCursor? = null,
        readRevision: Long? = null,
    ) = ScanRequest(prefix, readRevision = readRevision, cursor = cursor, pageSize = pageSize)

    private suspend fun Fixture.scanAll(pageSize: Int): List<ByteArray> {
        val first = assertIs<ScanResult.Page>(client.scanAtRevision(request(pageSize)))
        return first.entries.map { it.key } + scanFrom(first, pageSize).map { it.key }
    }

    // 残りのページを列挙する。途中で Complete（nextCursor=null）になるまで辿る。
    private suspend fun Fixture.scanFrom(first: ScanResult.Page, pageSize: Int): List<ScanEntry> {
        val out = mutableListOf<ScanEntry>()
        var page: ScanResult.Page = first
        while (true) {
            val cursor = page.nextCursor ?: break
            page = assertIs<ScanResult.Page>(client.scanAtRevision(request(pageSize, cursor = cursor)))
            out += page.entries
        }
        return out
    }

    private fun key(name: String, index: Int) = prefix + name.encodeToByteArray() + byteArrayOf(index.toByte())

    private fun value(index: Int) = byteArrayOf(index.toByte())

    private class Fixture(
        val runtime: RaftRuntime,
        val engine: MemoryMvccStorageEngine,
        val machine: StorageEngineStateMachine,
        val client: RevisionScanClient,
    )

    private suspend fun TestScope.fixture(elect: Boolean = true): Fixture {
        val engine = MemoryMvccStorageEngine()
        val machine = StorageEngineStateMachine(engine)
        val node = RaftNodeBuilder(
            "node-1",
            emptySet(),
            MemoryRaftPersistentStateStore(),
            RaftVolatileState(),
            MemoryRaftLog(),
            machine,
        ).build()
        val runtime = RaftRuntime(node, LocalRaftTransport(), backgroundScope.coroutineContext)
        if (elect) runtime.onElectionTimeout()
        return Fixture(runtime, engine, machine, RevisionScanClient(runtime))
    }
}
