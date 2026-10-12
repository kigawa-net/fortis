package net.kigawa.fortis.raft.log

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult

class FileRaftLog private constructor(
    private val file: FortisFile,
    private val codec: RaftLogCodec,
    private val index: MutableMap<Long, RaftLogIndexEntry>,
    private var fileSize: Long,
) : RaftLog {
    private val mutex = Mutex()

    override suspend fun lastIndex(): Long = mutex.withLock {
        index.keys.maxOrNull() ?: 0L
    }

    override suspend fun get(index: Long): RaftLogEntry? = mutex.withLock {
        if (index <= 0) return@withLock null
        val indexedEntry = this.index[index] ?: return@withLock null
        val bytes = readExact(indexedEntry.offset, indexedEntry.length)
        when (val result = codec.decode(bytes)) {
            is RaftLogDecodeResult.Success -> {
                if (result.entry.index != index) {
                    throw RaftLogCorruptionException(
                        "Raft log index mismatch at offset ${indexedEntry.offset}",
                    )
                }
                result.entry
            }

            RaftLogDecodeResult.Incomplete -> throw RaftLogCorruptionException(
                "Incomplete Raft log entry at offset ${indexedEntry.offset}",
            )

            is RaftLogDecodeResult.Corrupted -> throw RaftLogCorruptionException(
                "Corrupted Raft log at offset ${indexedEntry.offset}: ${result.reason}",
            )
        }
    }

    override suspend fun append(entry: RaftLogEntry) = mutex.withLock {
        val lastIndex = index.keys.maxOrNull() ?: 0L
        require(entry.index == lastIndex + 1) {
            "Raft log index must be contiguous"
        }

        val bytes = codec.encode(entry)
        val offset = fileSize
        file.openWrite(isCreate = true) { output ->
            var dataOffset = 0
            while (dataOffset < bytes.size) {
                val written = output.writeAt(
                    offset = offset + dataOffset,
                    data = bytes,
                    dataOffset = dataOffset,
                    length = bytes.size - dataOffset,
                )
                check(written > 0) { "Raft log write made no progress" }
                dataOffset += written
            }
            output.sync()
        }
        index[entry.index] = RaftLogIndexEntry(offset, bytes.size)
        fileSize += bytes.size
    }

    override suspend fun truncateFrom(index: Long) = mutex.withLock {
        require(index > 0) { "Raft log index must be positive" }
        val firstRemoved = this.index[index] ?: return@withLock
        file.openWrite(isCreate = true) { output ->
            output.truncate(firstRemoved.offset)
            output.sync()
        }
        val removedIndexes = this.index.keys.filter { it >= index }
        for (removedIndex in removedIndexes) {
            this.index.remove(removedIndex)
        }
        fileSize = firstRemoved.offset
    }

    private suspend fun readExact(offset: Long, length: Int): ByteArray {
        val bytes = ByteArray(length)
        file.openRead { input ->
            var dataOffset = 0
            while (dataOffset < bytes.size) {
                val read = input.readAt(
                    offset = offset + dataOffset,
                    buffer = bytes,
                    bufferOffset = dataOffset,
                    length = bytes.size - dataOffset,
                )
                if (read <= 0) {
                    throw RaftLogCorruptionException(
                        "Unexpected EOF in Raft log at offset ${offset + dataOffset}",
                    )
                }
                dataOffset += read
            }
        }
        return bytes
    }

    companion object {
        suspend fun open(
            file: FortisFile,
            codec: RaftLogCodec = RaftLogCodec(),
        ): FileRaftLog {
            val data = readAll(file) ?: run {
                file.openWrite(isCreate = true) { output -> output.sync() }
                return FileRaftLog(file, codec, mutableMapOf(), 0)
            }
            val index = linkedMapOf<Long, RaftLogIndexEntry>()
            var offset = 0
            while (offset < data.size) {
                when (val result = codec.decode(data, offset)) {
                    is RaftLogDecodeResult.Success -> {
                        val expectedIndex = index.size + 1L
                        if (result.entry.index != expectedIndex) {
                            throw RaftLogCorruptionException(
                                "Non-contiguous Raft log index at offset $offset: " +
                                    "expected $expectedIndex, found ${result.entry.index}",
                            )
                        }
                        index[result.entry.index] = RaftLogIndexEntry(
                            offset = offset.toLong(),
                            length = result.bytesRead,
                        )
                        offset += result.bytesRead
                    }

                    RaftLogDecodeResult.Incomplete -> {
                        file.openWrite(isCreate = true) { output ->
                            output.truncate(offset.toLong())
                            output.sync()
                        }
                        break
                    }

                    is RaftLogDecodeResult.Corrupted -> {
                        throw RaftLogCorruptionException(
                            "Corrupted Raft log at offset $offset: ${result.reason}",
                        )
                    }
                }
            }
            return FileRaftLog(file, codec, index, offset.toLong())
        }

        private suspend fun readAll(file: FortisFile): ByteArray? {
            var result: ByteArray? = null
            try {
                file.openRead { input ->
                    val size = input.size()
                    if (size > Int.MAX_VALUE) {
                        throw RaftLogCorruptionException(
                            "Raft log file is too large: $size",
                        )
                    }
                    val data = ByteArray(size.toInt())
                    var offset = 0
                    while (offset < data.size) {
                        val read = input.readAt(
                            offset = offset.toLong(),
                            buffer = data,
                            bufferOffset = offset,
                            length = data.size - offset,
                        )
                        if (read <= 0) {
                            throw RaftLogCorruptionException(
                                "Unexpected EOF in Raft log at offset $offset",
                            )
                        }
                        offset += read
                    }
                    result = data
                }
            } catch (_: FsFileNotFoundException) {
                return null
            }
            return result
        }
    }
}
