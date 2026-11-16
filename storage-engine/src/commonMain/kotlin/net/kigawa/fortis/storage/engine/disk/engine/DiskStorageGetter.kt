package net.kigawa.fortis.storage.engine.disk.engine

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsInput
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskCorruptionException
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry

class DiskStorageGetter(
    val mutex: Mutex,
    val index: Map<ByteArrayKey, DiskIndexEntry>,
    val file: FortisFile,
) {
    suspend fun get(key: ByteArray): ByteArray? = mutex.withLock {
        val entry = index[ByteArrayKey(key)] ?: return@withLock null
        val value = ByteArray(entry.valueLength)

        file.openRead { input ->
            var read = 0

            while (read < value.size) {
                read += readRow(input, entry, read, value)
            }
        }

        value
    }

    private suspend fun readRow(input: FsInput, entry: DiskIndexEntry, read: Int, value: ByteArray): Int {
        val count = input.readAt(
            offset = entry.valueOffset + read,
            buffer = value,
            bufferOffset = read,
            length = value.size - read,
        )

        if (count <= 0) {
            throw DiskCorruptionException(
                "Unexpected EOF at offset ${entry.valueOffset + read}"
            )
        }
        return count
    }

}