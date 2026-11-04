package net.kigawa.fortis.storage.engine.mvcc

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.storage.engine.BytewiseOrder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class MvccPrefixScanTest {
    private val a = byteArrayOf(0x61) // 'a'
    private val ab = byteArrayOf(0x61, 0x62)
    private val b = byteArrayOf(0x62)
    private val high = byteArrayOf(0x61, 0x80.toByte())

    private fun assertKeys(expected: List<ByteArray>, entries: List<MvccScanEntry>) {
        assertEquals(expected.map { it.toList() }, entries.map { it.key.toList() })
    }

    @Test
    fun scanReturnsVisibleLiveKeysInBytewiseOrderWithinPrefix() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(high, byteArrayOf(1), 1)
        storage.putAt(ab, byteArrayOf(2), 2)
        storage.putAt(a, byteArrayOf(3), 3)
        storage.putAt(b, byteArrayOf(4), 4) // prefix 外

        val page = assertIs<MvccScanResult.Page>(storage.scanPrefixAt(a, 4, null, 10))

        assertKeys(listOf(a, ab, high), page.entries)
        assertEquals(listOf(3L, 2L, 1L), page.entries.map { it.version })
        assertEquals(false, page.hasMore)
    }

    @Test
    fun pagesCoverEveryKeyExactlyOnceForAnyPageSize() = runTest {
        val storage = MemoryMvccStorageEngine()
        val keys = (0 until 50).map { byteArrayOf(0x10, it.toByte()) }
        keys.forEachIndexed { i, key -> storage.putAt(key, byteArrayOf(i.toByte()), i + 1L) }
        val latest = storage.latestVersion()

        for (limit in listOf(1, 7, 256)) {
            val collected = mutableListOf<ByteArray>()
            var startAfter: ByteArray? = null
            var hasMore = true
            while (hasMore) {
                val page = assertIs<MvccScanResult.Page>(
                    storage.scanPrefixAt(byteArrayOf(0x10), latest, startAfter, limit),
                )
                assertTrue(page.entries.size <= limit)
                collected += page.entries.map { it.key }
                startAfter = page.entries.lastOrNull()?.key
                hasMore = page.hasMore
            }

            assertEquals(keys.map { it.toList() }, collected.map { it.toList() }, "limit=$limit")
        }
    }

    @Test
    fun tombstonesAreHiddenAtAndAfterDeleteButVisibleBeforeIt() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(a, byteArrayOf(1), 1)
        storage.putAt(b, byteArrayOf(2), 2)
        storage.deleteAt(a, 3)

        val before = assertIs<MvccScanResult.Page>(storage.scanPrefixAt(byteArrayOf(), 2, null, 10))
        val after = assertIs<MvccScanResult.Page>(storage.scanPrefixAt(byteArrayOf(), 3, null, 10))

        assertKeys(listOf(a, b), before.entries)
        assertKeys(listOf(b), after.entries)
    }

    @Test
    fun scanAtFixedVersionIgnoresLaterWritesAndDeletes() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(a, byteArrayOf(1), 1)
        storage.putAt(b, byteArrayOf(2), 2)
        val pinned = storage.latestVersion()

        val first = assertIs<MvccScanResult.Page>(storage.scanPrefixAt(byteArrayOf(), pinned, null, 1))
        // 後続の書き込み・削除・新規キーは固定版には見えない
        storage.putAt(a, byteArrayOf(9), 3)
        storage.deleteAt(b, 4)
        storage.putAt(byteArrayOf(0x63), byteArrayOf(9), 5)
        val second = assertIs<MvccScanResult.Page>(
            storage.scanPrefixAt(byteArrayOf(), pinned, first.entries.last().key, 10),
        )

        assertKeys(listOf(a), first.entries)
        assertEquals(listOf(1L), first.entries.map { it.version })
        assertKeys(listOf(b), second.entries)
        assertEquals(listOf(2L), second.entries.map { it.version })
    }

    @Test
    fun compactionAtOrBelowReadVersionKeepsScanResultIntact() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(a, byteArrayOf(1), 1)
        storage.putAt(a, byteArrayOf(2), 2)
        storage.putAt(a, byteArrayOf(3), 3)
        storage.putAt(b, byteArrayOf(4), 4)

        storage.compact(3)

        // 版 3 時点の a は 3 のみ、b はまだ存在しない
        val page = assertIs<MvccScanResult.Page>(storage.scanPrefixAt(byteArrayOf(), 3, null, 10))
        assertKeys(listOf(a), page.entries)
        assertEquals(listOf(3L), page.entries.map { it.version })
        assertKeys(listOf(a, b), assertIs<MvccScanResult.Page>(storage.scanPrefixAt(byteArrayOf(), 4, null, 10)).entries)
    }

    @Test
    fun readVersionLostToCompactionReturnsCompactedWithoutPartialPage() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(a, byteArrayOf(1), 1)
        storage.putAt(b, byteArrayOf(2), 2)
        storage.putAt(a, byteArrayOf(3), 3)

        storage.compact(3)

        assertEquals(MvccScanResult.Compacted(3), storage.scanPrefixAt(byteArrayOf(), 2, null, 10))
    }

    @Test
    fun invalidArgumentsAreRejected() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(a, byteArrayOf(1), 1)

        assertFailsWith<IllegalArgumentException> { storage.scanPrefixAt(byteArrayOf(), 1, null, 0) }
        assertFailsWith<IllegalArgumentException> { storage.scanPrefixAt(byteArrayOf(), 2, null, 1) }
        assertFailsWith<IllegalArgumentException> { storage.scanPrefixAt(byteArrayOf(), -1, null, 1) }
    }

    @Test
    fun bytewiseOrderTreatsBytesAsUnsigned() {
        assertTrue(BytewiseOrder.compare(byteArrayOf(0x7f), byteArrayOf(0x80.toByte())) < 0)
        assertTrue(BytewiseOrder.compare(byteArrayOf(0x61), byteArrayOf(0x61, 0)) < 0)
        assertEquals(0, BytewiseOrder.compare(byteArrayOf(), byteArrayOf()))
    }
}
