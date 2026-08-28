package net.kigawa.fortis.storage.engine

@Suppress("unused")
class FortisStorageEngine {
    suspend fun get(key: ByteArray): ByteArray? {
        TODO()
    }

    suspend fun put(key: ByteArray, value: ByteArray) {
        TODO()
    }

    suspend fun delete(key: ByteArray): Boolean {
        TODO()
    }
}