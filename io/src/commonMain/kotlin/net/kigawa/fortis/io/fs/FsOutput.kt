package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Output

interface FsOutput: Output {
    val file: FortisFile

    suspend fun writeAt(
        offset: FsOffset, data: ByteArray,
        dataOffset: Int,
        length: Int,
    ): Int

    /**
     * Single-writer append only. This API is NOT atomic across independent file
     * descriptors/processes; the WAL/log owner must ensure exclusive path ownership.
     * A local Kotlin mutex is not a cross-process lock. Handle short writes explicitly.
     */
    suspend fun writeAppend(
        data: ByteArray,
        dataOffset: Int,
        length: Int,
    ): Int

    suspend fun sync()
    suspend fun truncate(size: FsByteSize)

}