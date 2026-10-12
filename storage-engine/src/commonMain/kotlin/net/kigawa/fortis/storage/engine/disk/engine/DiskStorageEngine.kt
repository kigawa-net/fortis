package net.kigawa.fortis.storage.engine.disk.engine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.builder.DiskStorageEngineBuilder
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec

class DiskStorageEngine(
    file: FortisFile,
    private val index: MutableMap<ByteArrayKey, DiskIndexEntry>,
    endOffset: Long,
    val diskCodec: DiskCodec,
): FortisStorageEngine {
    private val mutex = Mutex()
    val diskStorageGetter = DiskStorageGetter(mutex, index, file)
    val diskStorageAppender = DiskStorageAppender(endOffset, file)
    val diskStoragePutter = DiskStoragePutter(mutex, diskCodec, index, diskStorageAppender)

    companion object {
        val builder = ::DiskStorageEngineBuilder
    }

    override suspend fun get(key: ByteArray): ByteArray? = diskStorageGetter.get(key)

    override suspend fun put(key: ByteArray, value: ByteArray) = diskStoragePutter.put(key, value)

    override suspend fun delete(
        key: ByteArray,
    ): Boolean = mutex.withLock {
        if (!index.containsKey(ByteArrayKey(key))) {
            return@withLock false
        }

        diskStorageAppender.append(
            diskCodec.encoder.encodeDelete(key),
        )

        index.remove(ByteArrayKey(key))
        true
    }

}
