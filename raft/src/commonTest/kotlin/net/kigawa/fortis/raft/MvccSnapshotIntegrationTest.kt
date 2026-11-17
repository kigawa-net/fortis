package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import net.kigawa.fortis.storage.engine.mvcc.MvccCommitResult
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MvccSnapshotIntegrationTest {
    @Test
    fun snapshotAfterGcRestoresVersionStoreAcrossKeysAndSupportsFurtherWrites() = runTest {
        val source = MemoryMvccStorageEngine()
        val key = byteArrayOf(1)
        val otherKey = byteArrayOf(2)
        source.put(key, byteArrayOf(10))
        source.put(key, byteArrayOf(20))
        source.put(otherKey, byteArrayOf(30))
        val pin = source.retainLatest()
        source.delete(key)
        source.collectGarbage()
        val history = source.exportHistory()
        assertEquals(listOf(2L, 3L, 4L), history.map { it.version })
        val bytes = StorageEngineStateMachine(source).snapshotData()
        source.release(pin)
        source.put(otherKey, byteArrayOf(99))

        val restored = MemoryMvccStorageEngine()
        StorageEngineStateMachine(restored).restoreSnapshot(bytes)
        val restoredHistory = restored.exportHistory()
        assertEquals(history.map { it.version }, restoredHistory.map { it.version })
        history.zip(restoredHistory).forEach { (expected, actual) ->
            assertContentEquals(expected.key, actual.key)
            assertContentEquals(expected.value, actual.value)
        }
        assertContentEquals(byteArrayOf(20), restored.getAt(key, pin.version))
        assertNull(restored.get(key))
        assertContentEquals(byteArrayOf(30), restored.get(otherKey))
        val update = restored.beginWrite(key, byteArrayOf(40))
        assertEquals(5L, update.version)
        assertEquals(MvccCommitResult.COMMITTED, restored.commit(update))
        assertEquals(update.version, restored.versionOf(key))
        assertContentEquals(byteArrayOf(40), restored.get(key))
        assertTrue(restored.checkpoint().uncommittedVersions.isEmpty())
    }

    @Test
    fun stateMachineSnapshotRebuildsCommittedVersionsAndTimestampCounter() = runTest {
        val source = MemoryMvccStorageEngine()
        val key = byteArrayOf(1)
        source.putAt(key, byteArrayOf(10), 10)
        val update = source.beginWrite(key, byteArrayOf(20))
        assertEquals(MvccCommitResult.COMMITTED, source.commit(update))
        val deletion = source.beginWrite(key, null)
        assertEquals(MvccCommitResult.COMMITTED, source.commit(deletion))
        val bytes = StorageEngineStateMachine(source).snapshotData()

        val restored = MemoryMvccStorageEngine()
        StorageEngineStateMachine(restored).restoreSnapshot(bytes)
        assertEquals(deletion.version, restored.latestVersion())
        assertEquals(deletion.version, restored.versionOf(key))
        assertContentEquals(byteArrayOf(10), restored.getAt(key, 10))
        assertContentEquals(byteArrayOf(20), restored.getAt(key, update.version))
        assertNull(restored.getAt(key, deletion.version))
        val pin = restored.retainLatest()
        val write = restored.beginWrite(key, byteArrayOf(30))
        assertEquals(deletion.version + 1, write.version)
        assertEquals(MvccCommitResult.COMMITTED, restored.commit(write))
        restored.collectGarbage()
        assertNull(restored.getAt(key, pin.version))
        assertContentEquals(byteArrayOf(30), restored.get(key))
        restored.release(pin)
    }
}
