package net.kigawa.fortis.storage.engine.wal.file

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.WalCodec
import net.kigawa.fortis.storage.engine.wal.WalRecord

data class FileWal(
    val file: FortisFile,
    val codec: WalCodec,
): Wal {
    private val mutex = Mutex()
    override suspend fun append(record: WalRecord) {
        mutex.withLock {
            val data = codec.encode(record)

            file.openWrite(isCreate = true) { output ->
                var dataOffset = 0

                while (dataOffset < data.size) {
                    val written = output.writeAppend(
                        data = data,
                        dataOffset = dataOffset,
                        length = data.size - dataOffset,
                    )

                    check(written > 0) {
                        "WAL write made no progress"
                    }

                    dataOffset += written
                }
            }
        }
    }

    override suspend fun replay(
        handler: suspend (WalRecord) -> Unit,
    ) {
        TODO()
    }

    override suspend fun sync() {
        mutex.withLock {
            file.openWrite(isCreate = true) { output ->
                output.sync()
            }
        }
    }
}