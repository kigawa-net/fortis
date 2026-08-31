package net.kigawa.fortis.storage.engine.memory

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

class MemoryStorageEngineTest {
    @Test
    fun missingKeyReturnsNull() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()

        assertNull(storage.get(byteArrayOf(1, 2, 3)))
    }

    @Test
    fun putAndGetUsesByteContentAsKey() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()
        val key = byteArrayOf(1, 2, 3)
        val value = byteArrayOf(4, 5, 6)

        storage.put(key, value)
        key[0] = 9

        assertContentEquals(value, storage.get(byteArrayOf(1, 2, 3)))
    }

    @Test
    fun putOverwritesExistingValue() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()
        val key = byteArrayOf(1)

        storage.put(key, byteArrayOf(2))
        storage.put(key, byteArrayOf(3))

        assertContentEquals(byteArrayOf(3), storage.get(key))
    }

    @Test
    fun deleteReturnsWhetherAnEntryExisted() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()
        val key = byteArrayOf(1, 2)

        assertFalse(storage.delete(key))
        storage.put(key, byteArrayOf(3))
        assertTrue(storage.delete(byteArrayOf(1, 2)))
        assertFalse(storage.delete(key))
        assertNull(storage.get(key))
    }

    @Test
    fun putAndGetProtectStoredBytesFromMutation() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()
        val key = byteArrayOf(1)
        val input = byteArrayOf(2)

        storage.put(key, input)
        input[0] = 9
        val result = storage.get(key)!!
        result[0] = 8

        assertContentEquals(byteArrayOf(2), storage.get(key))
    }

    @Test
    fun concurrentOperationsKeepEntriesConsistent() = kotlinx.coroutines.test.runTest {
        val storage = MemoryStorageEngine()
        val entries = (0 until 100).map { index ->
            byteArrayOf(index.toByte()) to byteArrayOf((index * 2).toByte())
        }

        coroutineScope {
            entries.map { (key, value) ->
                async(Dispatchers.Default) {
                    storage.put(key, value)
                }
            }.awaitAll()
        }

        coroutineScope {
            entries.map { (key, value) ->
                async(Dispatchers.Default) {
                    assertContentEquals(value, storage.get(key))
                }
            }.awaitAll()
        }

        val deleted = coroutineScope {
            entries.map { (key, _) ->
                async(Dispatchers.Default) {
                    storage.delete(key)
                }
            }.awaitAll()
        }
        assertTrue(deleted.all { it })

        coroutineScope {
            entries.map { (key, _) ->
                async(Dispatchers.Default) {
                    assertNull(storage.get(key))
                }
            }.awaitAll()
        }
    }
}
