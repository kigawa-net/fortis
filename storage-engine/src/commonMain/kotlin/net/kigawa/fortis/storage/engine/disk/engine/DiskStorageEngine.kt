package net.kigawa.fortis.storage.engine.disk.engine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.builder.DiskStorageEngineBuilder
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec

class DiskStorageEngine(
    private val file: FortisFile,
    private val index: MutableMap<ByteArrayKey, DiskIndexEntry>,
    private var endOffset: Long,
    val diskCodec: DiskCodec,
): FortisStorageEngine {
    private val mutex = Mutex()
    val diskStorageGetter = DiskStorageGetter(mutex, index,file)

    companion object {
        val builder = ::DiskStorageEngineBuilder
    }

    override suspend fun get(key: ByteArray): ByteArray? = diskStorageGetter.get(key)

    override suspend fun put(
        key: ByteArray,
        value: ByteArray,
    ) {
        mutex.withLock {
            val record = diskCodec.encoder.encodePut(
                key = key,
                value = value,
            )

            val recordOffset = append(record)

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

    override suspend fun delete(
        key: ByteArray,
    ): Boolean = mutex.withLock {
        if (!index.containsKey(ByteArrayKey(key))) {
            return@withLock false
        }

        append(
            diskCodec.encoder.encodeDelete(key),
        )

        index.remove(ByteArrayKey(key))
        true
    }

    private suspend fun append(
        data: ByteArray,
    ): Long {
        val offset = endOffset

        file.openWrite(isCreate = true) { output ->
            var writtenTotal = 0

            while (writtenTotal < data.size) {
                val written = output.writeAt(
                    offset = offset + writtenTotal,
                    data = data,
                    dataOffset = writtenTotal,
                    length = data.size - writtenTotal,
                )

                check(written > 0) {
                    "Disk write made no progress"
                }

                writtenTotal += written
            }

            output.sync()
        }

        endOffset += data.size

        return offset
    }
}
