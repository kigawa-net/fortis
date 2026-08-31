package net.kigawa.fortis.io.fs

import java.nio.channels.FileChannel

data class JvmFsOutput(
    override val file: FortisFile,
    val channel: FileChannel,
): FsOutput {
}