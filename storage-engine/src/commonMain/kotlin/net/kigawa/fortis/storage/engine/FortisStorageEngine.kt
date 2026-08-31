package net.kigawa.fortis.storage.engine

interface FortisStorageEngine {
    suspend fun get(key: ByteArray): ByteArray?

    suspend fun put(key: ByteArray, value: ByteArray)

    suspend fun delete(key: ByteArray): Boolean
}