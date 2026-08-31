package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

actual suspend fun Fs.openRead(
    file: FortisFile, block: suspend (input: FsInput) -> Unit,
) {
    withContext(Dispatchers.IO) {
        FileChannel.open(file.path.toJvmPath(), StandardOpenOption.READ).use { channel ->
            block(JvmFsInput(file, channel))
        }
    }
}

actual suspend fun Fs.openWrite(
    file: FortisFile, block: suspend (output: FsOutput) -> Unit,
) {
    withContext(Dispatchers.IO) {
        FileChannel.open(file.path.toJvmPath(), StandardOpenOption.WRITE).use { channel ->
            block(JvmFsOutput(file, channel))
        }
    }
}

actual suspend fun Fs.openReadWrite(
    file: FortisFile, block: suspend (io: FsIo) -> Unit,
) {
    withContext(Dispatchers.IO) {
        FileChannel.open(
            file.path.toJvmPath(), StandardOpenOption.READ, StandardOpenOption.WRITE
        ).use { channel ->
            block(FsIo(JvmFsInput(file, channel), JvmFsOutput(file, channel)))
        }
    }
}