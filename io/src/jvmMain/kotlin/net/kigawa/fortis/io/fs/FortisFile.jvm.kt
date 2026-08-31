package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

actual suspend fun FortisFile.openRead(
    block: suspend (input: FsInput) -> Unit,
) {
    withContext(Dispatchers.IO) {
        FileChannel.open(path.toJvmPath(), StandardOpenOption.READ).use { channel ->
            block(JvmFsInput(this@openRead, channel))
        }
    }
}

actual suspend fun FortisFile.openWrite(
    isCreate: Boolean,
    block: suspend (output: FsOutput) -> Unit,
) {
    var options = arrayOf(StandardOpenOption.WRITE)
    if (isCreate) options += StandardOpenOption.CREATE

    withContext(Dispatchers.IO) {
        FileChannel.open(path.toJvmPath(), *options).use { channel ->
            block(JvmFsOutput(this@openWrite, channel))
        }
    }
}

actual suspend fun FortisFile.openReadWrite(
    isCreate: Boolean,
    block: suspend (io: FsIo) -> Unit,
) {
    var options = arrayOf(StandardOpenOption.READ, StandardOpenOption.WRITE)
    if (isCreate) options += StandardOpenOption.CREATE

    withContext(Dispatchers.IO) {
        FileChannel.open(path.toJvmPath(), *options).use { channel ->
            block(
                FsIo(
                    JvmFsInput(this@openReadWrite, channel),
                    JvmFsOutput(this@openReadWrite, channel)
                )
            )
        }
    }
}