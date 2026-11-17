package net.kigawa.fortis.storage.engine.mvcc

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MvccLifecycleTest {
    private val key = byteArrayOf(1)
    private val otherKey = byteArrayOf(2)

    @Test
    fun sequentialTimestampsAreStrictlyIncreasingWithoutPublishingVersions() = runTest {
        val storage = MemoryMvccStorageEngine()
        val timestamps = (1..100).map { storage.nextTimestamp() }
        assertTrue(timestamps.zipWithNext().all { (previous, next) -> next > previous })
        assertEquals(0L, storage.latestVersion())
        assertTrue(storage.exportHistory().isEmpty())
        assertEquals(timestamps.last() + 1, storage.beginWrite(key, byteArrayOf(1)).version)
    }

    @Test
    fun timestampsIncludeExplicitWritesReservationsAndAbortWithoutReuse() = runTest {
        val storage = MemoryMvccStorageEngine()
        assertEquals(1L, storage.nextTimestamp())
        storage.putAt(key, byteArrayOf(10), 100)
        val intent = storage.beginWrite(key, byteArrayOf(20))
        assertEquals(101L, intent.version)
        storage.abort(intent)
        storage.put(otherKey, byteArrayOf(30))
        assertEquals(102L, storage.versionOf(otherKey))
        storage.delete(otherKey)
        assertEquals(103L, storage.latestVersion())
        storage.putAt(key, byteArrayOf(5), 50)
        assertEquals(104L, storage.nextTimestamp())
    }

    @Test
    fun timestampAdvancesPastCompactionFloorEvenWithoutCommittedWrites() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.compact(100)
        assertEquals(101L, storage.nextTimestamp())
        val restored = MemoryMvccStorageEngine()
        restored.restoreCheckpoint(storage.checkpoint())
        assertEquals(102L, restored.nextTimestamp())
        restored.put(key, byteArrayOf(1))
        assertEquals(103L, restored.versionOf(key))
    }

    @Test
    fun concurrentTimestampRequestsAreUnique() = runTest {
        val storage = MemoryMvccStorageEngine()
        val timestamps = (1..100).map { async(Dispatchers.Default) { storage.nextTimestamp() } }.awaitAll()
        assertEquals((1L..100L).toList(), timestamps.sorted())
    }

    @Test
    fun exhaustedCounterDoesNotWrap() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(key, byteArrayOf(1), Long.MAX_VALUE)
        assertFailsWith<IllegalStateException> { storage.nextTimestamp() }
        assertFailsWith<IllegalStateException> { storage.put(key, byteArrayOf(2)) }
        assertEquals(Long.MAX_VALUE, storage.gcSafePoint())
        storage.collectGarbage()
        assertContentEquals(byteArrayOf(1), storage.get(key))
    }

    @Test
    fun intentIsInvisibleUntilCommitAndCannotBeReusedOrTransferred() = runTest {
        val storage = MemoryMvccStorageEngine()
        val mutableKey = key.copyOf()
        val mutableValue = byteArrayOf(10)
        val intent = storage.beginWrite(mutableKey, mutableValue)
        mutableKey[0] = 99
        mutableValue[0] = 99
        intent.value!![0] = 99
        assertEquals(1L, intent.version)
        assertEquals(0L, storage.latestVersion())
        assertNull(storage.get(key))
        assertTrue(storage.exportHistory().isEmpty())
        assertFailsWith<IllegalStateException> { MemoryMvccStorageEngine().commit(intent) }
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(intent))
        assertContentEquals(byteArrayOf(10), storage.get(key))
        assertEquals(intent.version, storage.latestVersion())
        assertEquals(intent.version, storage.versionOf(key))
        assertContentEquals(byteArrayOf(10), storage.getAt(key, intent.version))
        assertEquals(listOf(intent.version), storage.exportHistory().map { it.version })
        assertTrue(storage.checkpoint().uncommittedVersions.isEmpty())
        assertFailsWith<IllegalStateException> { storage.commit(intent) }
        assertFailsWith<IllegalStateException> { storage.abort(intent) }
        val deletion = storage.beginWrite(key, null)
        assertContentEquals(byteArrayOf(10), storage.get(key))
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(deletion))
        assertNull(storage.get(key))
        assertContentEquals(byteArrayOf(10), storage.getAt(key, intent.version))
    }

    @Test
    fun readConflictDetectsAbaAndDeletionByVersion() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.put(key, byteArrayOf(10))
        val read = storage.readVersioned(key)
        storage.put(key, byteArrayOf(20))
        storage.put(key, byteArrayOf(10))
        val intent = storage.beginWrite(otherKey, byteArrayOf(1), listOf(read))
        assertEquals(MvccCommitResult.READ_CONFLICT, storage.commit(intent))
        assertNull(storage.get(otherKey))
        val beforeDelete = storage.readVersioned(key)
        storage.delete(key)
        assertEquals(MvccCommitResult.READ_CONFLICT,
            storage.commit(storage.beginWrite(otherKey, byteArrayOf(2), listOf(beforeDelete))))
        val deleted = storage.readVersioned(key)
        assertNull(deleted.value)
        assertEquals(5L, deleted.version)
    }

    @Test
    fun missingKeyReadConflictsWithInsertAndDelete() = runTest {
        val storage = MemoryMvccStorageEngine()
        val read = storage.readVersioned(key)
        assertEquals(0L, read.version)
        val intent = storage.beginWrite(otherKey, byteArrayOf(1), listOf(read))
        storage.put(key, byteArrayOf(2))
        storage.delete(key)
        assertEquals(MvccCommitResult.READ_CONFLICT, storage.commit(intent))
    }

    @Test
    fun successfulReadValidationAndWriteConflictUseReservationOrder() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.put(otherKey, byteArrayOf(1))
        val first = storage.beginWrite(key, byteArrayOf(10), listOf(storage.readVersioned(otherKey)))
        val second = storage.beginWrite(key, byteArrayOf(20))
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(first))
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(second))
        assertContentEquals(byteArrayOf(20), storage.get(key))
        val older = storage.beginWrite(key, byteArrayOf(30))
        val newer = storage.beginWrite(key, byteArrayOf(40))
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(newer))
        assertEquals(MvccCommitResult.SUPERSEDED, storage.commit(older))
        assertContentEquals(byteArrayOf(40), storage.get(key))
    }

    @Test
    fun lateCommitCannotChangeAnAlreadyPublishedSnapshot() = runTest {
        val storage = MemoryMvccStorageEngine()
        val pending = storage.beginWrite(key, byteArrayOf(1))
        storage.put(otherKey, byteArrayOf(2))
        val snapshot = storage.retainLatest()
        assertNull(storage.getAt(key, snapshot.version))
        assertEquals(MvccCommitResult.SUPERSEDED, storage.commit(pending))
        assertNull(storage.getAt(key, snapshot.version))
        storage.release(snapshot)
    }

    @Test
    fun safePointTracksReferenceCountsAndGcRemovesOldHistoryAndStaleIntents() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.put(key, byteArrayOf(10))
        val pin = storage.retainLatest()
        val duplicate = storage.retain(pin.version)
        val aborted = storage.beginWrite(otherKey, byteArrayOf(20))
        storage.abort(aborted)
        storage.put(key, byteArrayOf(30))
        val newerPin = storage.retainLatest()
        storage.put(key, byteArrayOf(40))
        assertEquals(1L, storage.gcSafePoint())
        assertEquals(0, storage.collectGarbage())
        storage.release(pin)
        assertEquals(1L, storage.gcSafePoint())
        storage.release(duplicate)
        assertEquals(3L, storage.gcSafePoint())
        assertEquals(1, storage.collectGarbage())
        assertContentEquals(byteArrayOf(30), storage.getAt(key, newerPin.version))
        storage.release(newerPin)
        assertEquals(5L, storage.gcSafePoint())
        storage.collectGarbage()
        assertEquals(listOf(4L), storage.exportHistory().map { it.version })
        assertFailsWith<IllegalArgumentException> { storage.getAt(key, 1) }
        assertFailsWith<IllegalArgumentException> { storage.retain(1) }
        assertEquals(MvccScanResult.Compacted(4), storage.scanPrefixAt(byteArrayOf(), 1, null, 10))
        assertContentEquals(byteArrayOf(40), storage.getAt(key, 4))
        assertContentEquals(byteArrayOf(40), storage.get(key))
        assertTrue(storage.checkpoint().uncommittedVersions.isEmpty())
    }

    @Test
    fun checkpointRebuildsHistoryFloorCounterAndMakesCrashedIntentsCollectable() = runTest {
        val source = MemoryMvccStorageEngine()
        source.put(key, byteArrayOf(10))
        source.put(key, byteArrayOf(20))
        source.delete(key)
        source.put(otherKey, byteArrayOf(30))
        source.compact(2)
        source.retainLatest()
        val crashed = source.beginWrite(key, byteArrayOf(99))
        val checkpoint = source.checkpoint()
        checkpoint.history.first().key[0] = 99
        val restored = MemoryMvccStorageEngine()
        restored.restoreCheckpoint(checkpoint)
        assertEquals(4L, restored.latestVersion())
        assertEquals(6L, restored.gcSafePoint())
        assertContentEquals(byteArrayOf(20), restored.getAt(key, 2))
        assertNull(restored.getAt(key, 3))
        assertNull(restored.get(key))
        assertContentEquals(byteArrayOf(30), restored.get(otherKey))
        assertFailsWith<IllegalArgumentException> { restored.getAt(key, 1) }
        assertFailsWith<IllegalStateException> { restored.commit(crashed) }
        assertEquals(listOf(crashed.version), restored.checkpoint().uncommittedVersions)
        assertEquals(1, restored.collectGarbage())
        assertTrue(restored.checkpoint().uncommittedVersions.isEmpty())
        val next = restored.beginWrite(key, byteArrayOf(40))
        assertTrue(next.version > crashed.version)
        assertEquals(MvccCommitResult.COMMITTED, restored.commit(next))
        assertFailsWith<IllegalStateException> { restored.restoreCheckpoint(checkpoint) }
    }

    @Test
    fun activeIntentSurvivesGcAndAbortNeverPublishesData() = runTest {
        val storage = MemoryMvccStorageEngine()
        val intent = storage.beginWrite(key, byteArrayOf(1))
        assertEquals(0, storage.collectGarbage())
        assertEquals(MvccCommitResult.COMMITTED, storage.commit(intent))
        assertContentEquals(byteArrayOf(1), storage.get(key))
        val aborted = storage.beginWrite(key, byteArrayOf(2))
        storage.abort(aborted)
        assertEquals(intent.version, storage.latestVersion())
        assertEquals(intent.version, storage.versionOf(key))
        assertEquals(listOf(intent.version), storage.exportHistory().map { it.version })
        assertEquals(listOf(aborted.version), storage.checkpoint().uncommittedVersions)
        assertFailsWith<IllegalStateException> { storage.commit(aborted) }
        assertEquals(1, storage.collectGarbage())
        assertTrue(storage.checkpoint().uncommittedVersions.isEmpty())
        assertContentEquals(byteArrayOf(1), storage.get(key))
        assertEquals(aborted.version + 1, storage.nextTimestamp())
    }

    @Test
    fun safePointUsesOldestPinRegardlessOfAcquisitionAndReleaseOrder() = runTest {
        val storage = MemoryMvccStorageEngine()
        storage.putAt(key, byteArrayOf(10), 10)
        storage.putAt(key, byteArrayOf(20), 20)
        storage.putAt(key, byteArrayOf(30), 30)
        val newest = storage.retain(30)
        val oldest = storage.retain(10)
        val middle = storage.retain(20)
        assertEquals(10L, storage.gcSafePoint())
        storage.release(middle)
        assertEquals(10L, storage.gcSafePoint())
        storage.release(oldest)
        assertEquals(30L, storage.gcSafePoint())
        storage.collectGarbage()
        assertFailsWith<IllegalArgumentException> { storage.getAt(key, 10) }
        assertFailsWith<IllegalArgumentException> { storage.getAt(key, 20) }
        assertContentEquals(byteArrayOf(30), storage.getAt(key, newest.version))
        storage.release(newest)
        assertEquals(31L, storage.gcSafePoint())
    }

    @Test
    fun crashWithOnlyPendingIntentsRestoresThemAsStaleWithoutPublishingValues() = runTest {
        val source = MemoryMvccStorageEngine()
        val pending = source.beginWrite(key, byteArrayOf(10))
        assertEquals(0, source.collectGarbage())
        val restored = MemoryMvccStorageEngine()
        restored.restoreCheckpoint(source.checkpoint())
        assertEquals(0L, restored.latestVersion())
        assertNull(restored.get(key))
        assertTrue(restored.exportHistory().isEmpty())
        assertFailsWith<IllegalStateException> { restored.commit(pending) }
        assertFailsWith<IllegalStateException> { restored.abort(pending) }
        assertEquals(listOf(pending.version), restored.checkpoint().uncommittedVersions)
        assertEquals(1, restored.collectGarbage())
        assertEquals(0, restored.collectGarbage())
        assertTrue(restored.checkpoint().uncommittedVersions.isEmpty())
        assertEquals(pending.version + 1, restored.nextTimestamp())
    }
}
