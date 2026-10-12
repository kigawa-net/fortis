package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.nio.channels.FileChannel
import java.nio.file.NoSuchFileException
import java.nio.file.StandardOpenOption
import java.nio.file.Files
import java.nio.file.StandardCopyOption

actual suspend fun FortisFile.openRead(
    block: suspend (input: FsInput) -> Unit,
) {
    try {
        withContext(Dispatchers.IO) {
            FileChannel.open(
                path.toJvmPath(),
                StandardOpenOption.READ,
            ).use { channel ->
                block(
                    JvmFsInput(
                        this@openRead,
                        channel,
                    )
                )
            }
        }
    } catch (e: NoSuchFileException) {
        throw FsFileNotFoundException(
            file = this,
            cause = e,
        )
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

actual suspend fun FortisFile.atomicReplace(target: FortisFile) {
    withContext(Dispatchers.IO) {
        Files.move(path.toJvmPath(), target.path.toJvmPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }
}

actual suspend fun FortisFile.syncDirectory() {
    withContext(Dispatchers.IO) {
        FileChannel.open(path.toJvmPath(), StandardOpenOption.READ).use { it.force(true) }
    }
}

actual suspend fun FortisFile.deleteIfExists() {
    withContext(Dispatchers.IO) { Files.deleteIfExists(path.toJvmPath()) }
}
