package net.kigawa.fortis.raft

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.io.fs.openWrite
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateDecodeResult

class FileRaftPersistentStateStore(
    private val file: FortisFile,
    private val codec: RaftPersistentStateCodec = RaftPersistentStateCodec(),
) : RaftPersistentStateStore {
    private val mutex = Mutex()

    override suspend fun load(): RaftPersistentState = mutex.withLock {
        val data = readAll() ?: return@withLock RaftPersistentState()
        when (val result = codec.decode(data)) {
            is RaftPersistentStateDecodeResult.Success -> result.state
            RaftPersistentStateDecodeResult.Incomplete -> {
                throw RaftPersistentStateCorruptionException(
                    "Incomplete Raft persistent state",
                )
            }

            is RaftPersistentStateDecodeResult.Corrupted -> {
                throw RaftPersistentStateCorruptionException(result.reason)
            }
        }
    }

    override suspend fun save(term: Long, votedFor: String?) {
        val data = codec.encode(RaftPersistentState(term, votedFor))
        mutex.withLock {
            file.openWrite(isCreate = true) { output ->
                output.truncate(0)
                var offset = 0
                while (offset < data.size) {
                    val written = output.writeAt(
                        offset = offset.toLong(),
                        data = data,
                        dataOffset = offset,
                        length = data.size - offset,
                    )
                    check(written > 0) {
                        "Raft persistent state write made no progress"
                    }
                    offset += written
                }
                output.sync()
            }
        }
    }

    private suspend fun readAll(): ByteArray? {
        var result: ByteArray? = null
        try {
            file.openRead { input ->
                val size = input.size()
                if (size > Int.MAX_VALUE) {
                    throw RaftPersistentStateCorruptionException(
                        "Raft persistent state file is too large: $size",
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
                        throw RaftPersistentStateCorruptionException(
                            "Unexpected EOF in Raft persistent state at offset $offset",
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
