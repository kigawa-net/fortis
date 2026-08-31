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

actual suspend fun Fs.openReadWrite(
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

actual suspend fun Fs.readAt(
    input: FsInput, offset: FsOffset, buffer: ByteArray,
): Int {
    TODO("Not yet implemented")
}

actual suspend fun Fs.writeAt(
    output: FsOutput, offset: FsOffset, data: ByteArray,
): Int {
    TODO()
}

actual suspend fun Fs.writeAppend(
    output: FsOutput, data: ByteArray,
): FsOffset {
    TODO("Not yet implemented")
}

actual suspend fun Fs.sync(output: FsOutput) {
}

actual suspend fun Fs.truncate(
    output: FsOutput, size: FsSize,
) {
}