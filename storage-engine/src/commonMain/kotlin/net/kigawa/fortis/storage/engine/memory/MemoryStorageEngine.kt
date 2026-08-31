package net.kigawa.fortis.storage.engine.memory

import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.FortisStorageEngine

class MemoryStorageEngine: FortisStorageEngine {
    private val data = mutableMapOf<ByteArrayKey, ByteArray>()

    override suspend fun get(key: ByteArray): ByteArray? {
        return data[ByteArrayKey(key)]?.copyOf()
    }

    override suspend fun put(key: ByteArray, value: ByteArray) {
        data[ByteArrayKey(key.copyOf())] = value.copyOf()
    }

    override suspend fun delete(key: ByteArray): Boolean {
        return data.remove(ByteArrayKey(key)) != null
    }
}