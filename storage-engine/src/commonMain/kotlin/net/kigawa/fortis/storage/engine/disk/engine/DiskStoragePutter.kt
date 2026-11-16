package net.kigawa.fortis.storage.engine.disk.engine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec

class DiskStoragePutter(
    val mutex: Mutex,
    val diskCodec: DiskCodec,
    val index: MutableMap<ByteArrayKey, DiskIndexEntry>,
    val diskStorageAppender: DiskStorageAppender,
) {

    suspend fun put(
        key: ByteArray,
        value: ByteArray,
    ) = mutex.withLock {
        val record = diskCodec.encoder.encodePut(
            key = key,
            value = value,
        )

        val recordOffset = diskStorageAppender.append(record)

        index[ByteArrayKey(key.copyOf())] =
            DiskIndexEntry(
                valueOffset =
                    recordOffset +
                        diskCodec.headerSize +
                        key.size,
                valueLength = value.size,
            )
    }

}