package net.kigawa.fortis.storage.engine.memory

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.FortisStorageEngine

class MemoryStorageEngine: FortisStorageEngine {
    private val data = mutableMapOf<ByteArrayKey, ByteArray>()
    private val mutex = Mutex()

    override suspend fun get(key: ByteArray): ByteArray? {
        return mutex.withLock { data[ByteArrayKey(key)]?.copyOf() }
    }

    override suspend fun put(key: ByteArray, value: ByteArray) {
        mutex.withLock { data[ByteArrayKey(key.copyOf())] = value.copyOf() }
    }

    override suspend fun delete(key: ByteArray): Boolean {
        return mutex.withLock { data.remove(ByteArrayKey(key)) != null }
    }
}