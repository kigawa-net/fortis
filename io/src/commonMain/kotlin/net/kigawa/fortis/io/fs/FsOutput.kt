package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Output

interface FsOutput: Output {
    val file: FortisFile

    suspend fun writeAt(
        offset: FsOffset, data: ByteArray,
        dataOffset: Int,
        length: Int,
    ): Int

    suspend fun writeAppend(
        data: ByteArray,
        dataOffset: Int,
        length: Int,
    ): Int

    suspend fun sync()
    suspend fun truncate(size: FsByteSize)

}