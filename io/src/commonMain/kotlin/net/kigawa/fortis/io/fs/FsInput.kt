package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Input

interface FsInput: Input {
    val file: FortisFile
    suspend fun readAt(
        offset: FsOffset,
        buffer: ByteArray,
        bufferOffset: Int,
        length: Int,
    ): Int
    suspend fun size(): FsByteSize
}