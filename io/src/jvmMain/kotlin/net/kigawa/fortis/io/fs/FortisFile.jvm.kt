package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

actual suspend fun FortisFile.openRead(
    file: FortisFile, block: suspend (input: FsInput) -> Unit,
) {
    withContext(Dispatchers.IO) {
        FileChannel.open(file.path.toJvmPath(), StandardOpenOption.READ).use { channel ->
            block(JvmFsInput(file, channel))
        }
    }
}

actual suspend fun FortisFile.openWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (output: FsOutput) -> Unit,
) {
    var options = arrayOf(StandardOpenOption.WRITE)
    if (isCreate) options += StandardOpenOption.CREATE

    withContext(Dispatchers.IO) {
        FileChannel.open(file.path.toJvmPath(), *options).use { channel ->
            block(JvmFsOutput(file, channel))
        }
    }
}

actual suspend fun FortisFile.openReadWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (io: FsIo) -> Unit,
) {
    var options = arrayOf(StandardOpenOption.READ, StandardOpenOption.WRITE)
    if (isCreate) options += StandardOpenOption.CREATE

    withContext(Dispatchers.IO) {
        FileChannel.open(file.path.toJvmPath(), *options).use { channel ->
            block(FsIo(JvmFsInput(file, channel), JvmFsOutput(file, channel)))
        }
    }
}