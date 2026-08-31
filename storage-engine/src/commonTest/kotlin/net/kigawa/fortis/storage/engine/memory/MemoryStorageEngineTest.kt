package net.kigawa.fortis.storage.engine.memory

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

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
}
