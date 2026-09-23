package net.kigawa.fortis.raft.log

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class MemoryRaftLogTest {
    @Test
    fun emptyLogHasLastIndexZero() = runTest {
        assertEquals(0L, MemoryRaftLog().lastIndex())
    }

    @Test
    fun appendedEntryCanBeRead() = runTest {
        val log = MemoryRaftLog()
        val entry = entry(1)

        log.append(entry)

        assertEquals(entry, log.get(1))
        assertEquals(1L, log.lastIndex())
    }

    @Test
    fun multipleEntriesCanBeAppended() = runTest {
        val log = MemoryRaftLog()
        val entries = (1L..3L).map(::entry)

        for (entry in entries) {
            log.append(entry)
        }

        assertEquals(entries, entries.indices.map { log.get(it + 1L) })
        assertEquals(3L, log.lastIndex())
    }

    @Test
    fun nonContiguousAppendIsRejected() = runTest {
        val log = MemoryRaftLog()
        log.append(entry(1))

        assertFailsWith<IllegalArgumentException> {
            log.append(entry(3))
        }

        assertEquals(1L, log.lastIndex())
        assertNull(log.get(3))
    }

    @Test
    fun truncateFromMiddleRemovesEntryAndEverythingAfterIt() = runTest {
        val log = logWithEntries(3)

        log.truncateFrom(2)

        assertEquals(entry(1), log.get(1))
        assertNull(log.get(2))
        assertNull(log.get(3))
        assertEquals(1L, log.lastIndex())
    }

    @Test
    fun truncateFromFirstEntryEmptiesLog() = runTest {
        val log = logWithEntries(3)

        log.truncateFrom(1)

        assertEquals(0L, log.lastIndex())
        assertNull(log.get(1))
    }

    @Test
    fun truncateFromFutureIndexDoesNothing() = runTest {
        val log = logWithEntries(2)

        log.truncateFrom(3)

        assertEquals(entry(1), log.get(1))
        assertEquals(entry(2), log.get(2))
        assertEquals(2L, log.lastIndex())
    }

    @Test
    fun appendCanContinueAfterTruncation() = runTest {
        val log = logWithEntries(3)
        log.truncateFrom(2)
        val replacement = entry(index = 2, term = 2)

        log.append(replacement)

        assertEquals(replacement, log.get(2))
        assertEquals(2L, log.lastIndex())
        assertNull(log.get(3))
    }

    @Test
    fun nonPositiveIndexReturnsNull() = runTest {
        val log = logWithEntries(1)

        assertNull(log.get(0))
        assertNull(log.get(-1))
    }

    private suspend fun logWithEntries(count: Int): MemoryRaftLog =
        MemoryRaftLog().also { log ->
            for (index in 1L..count.toLong()) {
                log.append(entry(index))
            }
        }

    private fun entry(
        index: Long,
        term: Long = 1,
    ) = RaftLogEntry(
        index = index,
        term = term,
        command = RaftCommand.Put(
            key = byteArrayOf(index.toByte()),
            value = byteArrayOf((index * 10).toByte()),
        ),
    )
}
