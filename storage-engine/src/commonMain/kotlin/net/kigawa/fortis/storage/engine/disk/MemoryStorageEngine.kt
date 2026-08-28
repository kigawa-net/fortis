package net.kigawa.fortis.storage.engine.disk

import net.kigawa.fortis.storage.engine.FortisStorageEngine

class MemoryStorageEngine: FortisStorageEngine {
    override suspend fun get(key: ByteArray): ByteArray? {
        TODO("Not yet implemented")
    }

    override suspend fun put(key: ByteArray, value: ByteArray) {
        TODO("Not yet implemented")
    }

    override suspend fun delete(key: ByteArray): Boolean {
        TODO("Not yet implemented")
    }
}