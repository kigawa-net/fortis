package net.kigawa.fortis.raft.log

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult

import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.snapshot.replaceSnapshotFile
import net.kigawa.fortis.raft.snapshot.snapshotChecksum
import net.kigawa.fortis.raft.snapshot.syncSnapshotFile

class FileRaftLog private constructor(
    private val file: FortisFile,
    private val codec: RaftLogCodec,
    private val index: MutableMap<Long, RaftLogIndexEntry>,
    private var fileSize: Long,
    private var base: SnapshotMetadata? = null,
) : RaftLog {
    private val mutex = Mutex()
    private var publicationFailed = false

    private fun checkPublication() {
        check(!publicationFailed) { "Raft log publication failed; reopen before continuing" }
    }

    override suspend fun snapshotMetadata(): SnapshotMetadata? = mutex.withLock { checkPublication(); base }

    override suspend fun compactThrough(metadata: SnapshotMetadata, appliedIndex: Long) = mutex.withLock {
        checkPublication()
        require(appliedIndex >= metadata.lastIncludedIndex) { "Snapshot is not applied" }
        require(metadata.lastIncludedIndex >= (base?.lastIncludedIndex ?: 0))
        if (metadata == base) return@withLock
        require(base?.lastIncludedIndex != metadata.lastIncludedIndex) { "Snapshot boundary term mismatch" }
        val boundary = index[metadata.lastIncludedIndex]?.let { codec.decode(readExact(it.offset, it.length)) }
        val matches = (boundary as? RaftLogDecodeResult.Success)?.entry?.term == metadata.lastIncludedTerm
        val kept = if (matches) index.filterKeys { it > metadata.lastIncludedIndex }.toList().sortedBy { it.first }.toMap() else emptyMap()
        val bytes = ByteArray(24 + kept.values.sumOf { it.length })
        BASE_MAGIC.copyInto(bytes)
        writeBoundaryLong(bytes, 4, metadata.lastIncludedIndex)
        writeBoundaryLong(bytes, 12, metadata.lastIncludedTerm)
        val nextIndex = linkedMapOf<Long, RaftLogIndexEntry>()
        val checksum = snapshotChecksum(bytes, 20)
        for (i in 0 until 4) bytes[20 + i] = (checksum ushr (24 - i * 8)).toByte()
        var offset = 24
        for ((entryIndex, entry) in kept) {
            readExact(entry.offset, entry.length).copyInto(bytes, offset)
            nextIndex[entryIndex] = RaftLogIndexEntry(offset.toLong(), entry.length)
            offset += entry.length
        }
        try {
            replaceSnapshotFile(file, bytes)
        } catch (failure: Throwable) {
            publicationFailed = true
            throw failure
        }
        index.clear()
        index.putAll(nextIndex)
        fileSize = bytes.size.toLong()
        base = metadata
    }

    override suspend fun lastIndex(): Long = mutex.withLock {
        checkPublication()
        index.keys.maxOrNull() ?: base?.lastIncludedIndex ?: 0L
    }

    override suspend fun get(index: Long): RaftLogEntry? = mutex.withLock {
        checkPublication()
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
        checkPublication()
        val lastIndex = index.keys.maxOrNull() ?: base?.lastIncludedIndex ?: 0L
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
        checkPublication()
        require(index > (base?.lastIncludedIndex ?: 0)) { "Raft log index must be positive" }
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
        private val BASE_MAGIC = byteArrayOf(70, 82, 76, 66)

        private fun writeBoundaryLong(bytes: ByteArray, offset: Int, value: Long) {
            for (i in 0 until 8) bytes[offset + i] = (value ushr (56 - i * 8)).toByte()
        }

        private fun readBoundaryLong(bytes: ByteArray, offset: Int): Long {
            var value = 0L
            for (i in 0 until 8) value = (value shl 8) or (bytes[offset + i].toLong() and 255L)
            return value
        }

        suspend fun open(
            file: FortisFile,
            codec: RaftLogCodec = RaftLogCodec(),
        ): FileRaftLog {
            val data = readAll(file) ?: run {
                file.openWrite(isCreate = true) { output -> output.sync() }
                return FileRaftLog(file, codec, mutableMapOf(), 0)
            }
            val index = linkedMapOf<Long, RaftLogIndexEntry>()
            var base: SnapshotMetadata? = null
            var offset = 0
            if (data.size >= 4 && data.copyOfRange(0, 4).contentEquals(BASE_MAGIC)) {
                if (data.size < 24) throw RaftLogCorruptionException("Incomplete Raft log boundary")
                base = try {
                    SnapshotMetadata(readBoundaryLong(data, 4), readBoundaryLong(data, 12))
                } catch (cause: IllegalArgumentException) {
                    throw RaftLogCorruptionException("Invalid Raft log boundary")
                }
                var checksum = 0
                for (i in 20 until 24) checksum = (checksum shl 8) or (data[i].toInt() and 255)
                if (checksum != snapshotChecksum(data, 20)) throw RaftLogCorruptionException("Corrupted Raft log boundary")
                offset = 24
            }
            while (offset < data.size) {
                when (val result = codec.decode(data, offset)) {
                    is RaftLogDecodeResult.Success -> {
                        val expectedIndex = (base?.lastIncludedIndex ?: 0L) + index.size + 1L
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
            if (base != null) syncSnapshotFile(file)
            return FileRaftLog(file, codec, index, offset.toLong(), base)
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
