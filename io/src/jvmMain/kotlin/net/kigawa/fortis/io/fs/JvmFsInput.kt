package net.kigawa.fortis.io.fs

import java.nio.channels.FileChannel

data class JvmFsInput(
    override val file: FortisFile,
    val channel: FileChannel,
): FsInput {
}