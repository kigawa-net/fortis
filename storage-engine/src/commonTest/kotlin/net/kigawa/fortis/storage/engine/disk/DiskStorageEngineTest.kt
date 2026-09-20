package net.kigawa.fortis.storage.engine.disk

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.WalOperation
import net.kigawa.fortis.storage.engine.wal.WalRecord
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DiskStorageEngineTest {
    @Test
    fun putThenGetReturnsValue() = runTest {
        val wal = TestWal()
        val storage = build(wal)

        storage.put(byteArrayOf(1), byteArrayOf(2))

        assertContentEquals(byteArrayOf(2), storage.get(byteArrayOf(1)))
        assertEquals(listOf(putRecord(1, 1, 2)), wal.records())
        assertEquals(1, wal.syncCount)
    }

    @Test
    fun putThenRebuildRestoresValue() = runTest {
        val wal = TestWal()
        build(wal).put(byteArrayOf(1), byteArrayOf(2))

        val rebuilt = build(wal)

        assertContentEquals(byteArrayOf(2), rebuilt.get(byteArrayOf(1)))
    }

    @Test
    fun deleteThenRebuildDoesNotRestoreValue() = runTest {
        val wal = TestWal()
        val storage = build(wal)
        storage.put(byteArrayOf(1), byteArrayOf(2))

        assertTrue(storage.delete(byteArrayOf(1)))
        val rebuilt = build(wal)

        assertNull(rebuilt.get(byteArrayOf(1)))
        assertEquals(WalRecord(2, WalOperation.DELETE, byteArrayOf(1), null), wal.records().last())
        assertEquals(2, wal.syncCount)
    }

    @Test
    fun multipleRecordsReplayInOrder() = runTest {
        val records = listOf(
            putRecord(1, 1, 10),
            putRecord(2, 2, 20),
            putRecord(3, 1, 11),
            WalRecord(4, WalOperation.DELETE, byteArrayOf(2), null),
            putRecord(5, 3, 30),
            WalRecord(6, WalOperation.DELETE, byteArrayOf(3), null),
            putRecord(7, 3, 31),
        )
        val wal = TestWal(records)

        val rebuilt = build(wal)

        assertContentEquals(byteArrayOf(11), rebuilt.get(byteArrayOf(1)))
        assertNull(rebuilt.get(byteArrayOf(2)))
        assertContentEquals(byteArrayOf(31), rebuilt.get(byteArrayOf(3)))
        assertEquals(records, wal.records())
        assertEquals(0, wal.syncCount)
    }

    @Test
    fun sequenceContinuesAfterRebuild() = runTest {
        val wal = TestWal(listOf(putRecord(40, 1, 10)))
        val storage = build(wal)
        storage.put(byteArrayOf(2), byteArrayOf(20))
        assertTrue(storage.delete(byteArrayOf(1)))

        val rebuilt = build(wal)
        rebuilt.put(byteArrayOf(3), byteArrayOf(30))

        assertEquals(listOf(40L, 41L, 42L, 43L), wal.records().map { it.sequence })
        assertContentEquals(byteArrayOf(30), build(wal).get(byteArrayOf(3)))
    }

    @Test
    fun concurrentPutsPreserveEveryRecordAndSequence() = runTest {
        val wal = TestWal()
        val storage = build(wal)
        val start = CompletableDeferred<Unit>()

        coroutineScope {
            repeat(100) { index ->
                launch(Dispatchers.Default) {
                    start.await()
                    storage.put(byteArrayOf(index.toByte()), byteArrayOf((index * 2).toByte()))
                }
            }
            start.complete(Unit)
        }

        val records = wal.records()
        assertEquals((1L..100L).toList(), records.map { it.sequence })
        assertEquals(100, wal.syncCount)
        val rebuilt = build(wal)
        repeat(100) { index ->
            val key = byteArrayOf(index.toByte())
            val value = byteArrayOf((index * 2).toByte())
            assertContentEquals(value, storage.get(key))
            assertContentEquals(value, rebuilt.get(key))
        }
    }

    @Test
    fun appendFailurePoisonsEngineOnPut() = runTest {
        assertFailurePoisonsEngine(failSync = false, delete = false)
    }

    @Test
    fun syncFailurePoisonsEngineOnPut() = runTest {
        assertFailurePoisonsEngine(failSync = true, delete = false)
    }

    @Test
    fun appendFailurePoisonsEngineOnDelete() = runTest {
        assertFailurePoisonsEngine(failSync = false, delete = true)
    }

    @Test
    fun syncFailurePoisonsEngineOnDelete() = runTest {
        assertFailurePoisonsEngine(failSync = true, delete = true)
    }

    private suspend fun assertFailurePoisonsEngine(failSync: Boolean, delete: Boolean) {
        val wal = TestWal()
        val storage = build(wal)
        val key = byteArrayOf(1)
        storage.put(key, byteArrayOf(10))
        storage.put(byteArrayOf(2), byteArrayOf(20))
        val before = wal.records()
        val failure = IllegalStateException("Injected WAL failure")
        if (failSync) wal.syncFailure = failure else wal.appendFailure = failure

        val thrown = assertFailsWith<IllegalStateException> {
            if (delete) storage.delete(key) else storage.put(key, byteArrayOf(99))
        }

        assertSame(failure, thrown)
        assertContentEquals(byteArrayOf(10), storage.get(key))
        assertContentEquals(byteArrayOf(20), storage.get(byteArrayOf(2)))
        assertEquals(2, wal.syncCount)
        assertEquals(before.size + if (failSync) 1 else 0, wal.records().size)

        val recordsAfterFailure = wal.records()
        wal.appendFailure = null
        wal.syncFailure = null

        assertSame(failure, assertFailsWith<IllegalStateException> {
            storage.put(byteArrayOf(3), byteArrayOf(30))
        }.cause)
        assertSame(failure, assertFailsWith<IllegalStateException> {
            storage.delete(key)
        }.cause)
        assertSame(failure, assertFailsWith<IllegalStateException> {
            storage.delete(byteArrayOf(3))
        }.cause)

        assertNull(storage.get(byteArrayOf(3)))
        assertContentEquals(byteArrayOf(10), storage.get(key))
        assertContentEquals(byteArrayOf(20), storage.get(byteArrayOf(2)))
        assertEquals(recordsAfterFailure, wal.records())
        assertEquals(2, wal.syncCount)
    }

    private suspend fun build(wal: Wal) = DiskStorageEngineBuilder().wal(wal).build()

    private fun putRecord(sequence: Long, key: Int, value: Int) = WalRecord(
        sequence, WalOperation.PUT, byteArrayOf(key.toByte()), byteArrayOf(value.toByte()),
    )

    private class TestWal(initialRecords: List<WalRecord> = emptyList()): Wal {
        private val mutex = Mutex()
        private val entries = initialRecords.toMutableList()
        var appendFailure: Exception? = null
        var syncFailure: Exception? = null
        var syncCount = 0
            private set

        override suspend fun append(record: WalRecord) {
            yield()
            mutex.withLock {
                appendFailure?.let { throw it }
                entries.add(record.copy(key = record.key.copyOf(), value = record.value?.copyOf()))
            }
        }

        override suspend fun sync() {
            yield()
            mutex.withLock {
                syncFailure?.let { throw it }
                syncCount++
            }
        }

        override suspend fun replay(handler: suspend (WalRecord) -> Unit) {
            for (record in records()) handler(record)
        }

        suspend fun records(): List<WalRecord> = mutex.withLock { entries.toList() }
    }
}
