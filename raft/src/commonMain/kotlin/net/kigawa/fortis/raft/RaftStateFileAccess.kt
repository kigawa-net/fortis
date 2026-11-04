package net.kigawa.fortis.raft

import net.kigawa.fortis.io.fs.*

/** File boundary used to exercise interruption at each publication stage. */
internal interface RaftStateFileAccess {
    suspend fun read(file: FortisFile): ByteArray?
    suspend fun writeAndSync(file: FortisFile, data: ByteArray)
    suspend fun syncFile(file: FortisFile)
    suspend fun atomicReplace(source: FortisFile, target: FortisFile)
    suspend fun syncDirectory(directory: FortisFile)
    suspend fun deleteIfExists(file: FortisFile)
}

internal object FortisRaftStateFileAccess : RaftStateFileAccess {
    override suspend fun read(file: FortisFile): ByteArray? {
        var result: ByteArray? = null
        try {
            file.openRead { input ->
                val size = input.size()
                if (size < 0 || size > Int.MAX_VALUE) {
                    throw RaftPersistentStateCorruptionException("Invalid Raft persistent state size: $size")
                }
                val data = ByteArray(size.toInt())
                var offset = 0
                while (offset < data.size) {
                    val count = input.readAt(offset.toLong(), data, offset, data.size - offset)
                    if (count <= 0) throw RaftPersistentStateCorruptionException("Unexpected EOF at offset $offset")
                    offset += count
                }
                result = data
            }
        } catch (_: FsFileNotFoundException) {
            return null
        }
        return result
    }

    override suspend fun writeAndSync(file: FortisFile, data: ByteArray) {
        file.openWrite(isCreate = true) { output ->
            output.truncate(0)
            var offset = 0
            while (offset < data.size) {
                val written = output.writeAt(offset.toLong(), data, offset, data.size - offset)
                check(written > 0) { "Raft persistent state write made no progress" }
                offset += written
            }
            output.sync()
        }
    }

    override suspend fun syncFile(file: FortisFile) { file.openWrite(isCreate = false) { it.sync() } }
    override suspend fun atomicReplace(source: FortisFile, target: FortisFile) { source.atomicReplace(target) }
    override suspend fun syncDirectory(directory: FortisFile) { directory.syncDirectory() }
    override suspend fun deleteIfExists(file: FortisFile) { file.deleteIfExists() }
}
