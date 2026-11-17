package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import net.kigawa.fortis.storage.engine.mvcc.MvccCommitResult
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

class MvccSnapshotIntegrationTest {
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
