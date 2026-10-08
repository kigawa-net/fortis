package net.kigawa.fortis.storage.engine.mvcc

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MemoryMvccStorageEngineTest {
    @Test
    fun versionedReadsAndDeleteKeepHistory() = runTest {
        val storage = MemoryMvccStorageEngine()
        val key = byteArrayOf(1)

        storage.putAt(key, byteArrayOf(10), 1)
        storage.putAt(key, byteArrayOf(20), 2)

        // 旧版・新版・最新がそれぞれ正しい
        assertContentEquals(byteArrayOf(10), storage.getAt(key, 1))
        assertContentEquals(byteArrayOf(20), storage.getAt(key, 2))
        assertContentEquals(byteArrayOf(20), storage.get(key))

        storage.deleteAt(key, 3)

        // 削除後も旧版は残り、最新は null
        assertNull(storage.getAt(key, 3))
        assertNull(storage.get(key))
        assertContentEquals(byteArrayOf(20), storage.getAt(key, 2))
    }

    @Test
    fun snapshotAndCompactKeepLatestReads() = runTest {
        val storage = MemoryMvccStorageEngine()
        val key = byteArrayOf(1)

        storage.putAt(key, byteArrayOf(10), 1)
        storage.putAt(key, byteArrayOf(20), 2)

        // スナップショット版で一貫読みできる
        val snapshot = storage.snapshot()
        assertEquals(2L, snapshot.version)
        assertContentEquals(byteArrayOf(20), storage.getAt(key, snapshot.version))
        assertEquals(2L, storage.latestVersion())

        storage.compact(1)

        // 圧縮後も最新読みは壊れない
        assertContentEquals(byteArrayOf(20), storage.get(key))
        assertContentEquals(byteArrayOf(20), storage.getAt(key, 2))
        assertEquals(2L, storage.latestVersion())
    }
}
