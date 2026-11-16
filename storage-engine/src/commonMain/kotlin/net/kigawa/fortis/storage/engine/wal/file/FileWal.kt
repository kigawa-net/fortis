package net.kigawa.fortis.storage.engine.wal.file

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.codec.WalCodec
import net.kigawa.fortis.storage.engine.wal.codec.WalDecodeResult
import net.kigawa.fortis.storage.engine.wal.codec.WalRecord

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
        mutex.withLock {
            file.openRead { input ->
                val size = input.size()

                require(size <= Int.MAX_VALUE) {
                    "WAL file too large: $size"
                }

                val data = ByteArray(size.toInt())

                var readOffset = 0
                while (readOffset < data.size) {
                    val read = input.readAt(
                        offset = readOffset.toLong(),
                        buffer = data,
                        bufferOffset = readOffset,
                        length = data.size - readOffset,
                    )

                    check(read > 0) {
                        "WAL read made no progress"
                    }

                    readOffset += read
                }

                var offset = 0

                while (offset < data.size) {
                    when (val result = codec.decode(data, offset)) {
                        is WalDecodeResult.Success -> {
                            handler(result.record)
                            offset += result.bytesRead
                        }

                        WalDecodeResult.Incomplete -> {
                            break
                        }

                        is WalDecodeResult.Corrupted -> {
                            error(
                                "Corrupted WAL at offset $offset: ${result.reason}"
                            )
                        }
                    }
                }
            }
        }
    }

    override suspend fun sync() {
        mutex.withLock {
            file.openWrite(isCreate = true) { output ->
                output.sync()
            }
        }
    }
}