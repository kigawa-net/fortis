@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package net.kigawa.fortis.io.fs

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import platform.posix.*

actual suspend fun FortisFile.openRead(block: suspend (input: FsInput) -> Unit) =
    withPosixFile(O_RDONLY) { block(PosixInput(this, it)) }

actual suspend fun FortisFile.openWrite(isCreate: Boolean, block: suspend (output: FsOutput) -> Unit) =
    withPosixFile(O_WRONLY or if (isCreate) O_CREAT else 0) { block(PosixOutput(this, it)) }

actual suspend fun FortisFile.openReadWrite(isCreate: Boolean, block: suspend (io: FsIo) -> Unit) =
    withPosixFile(O_RDWR or if (isCreate) O_CREAT else 0) { block(FsIo(PosixInput(this, it), PosixOutput(this, it))) }

actual suspend fun FortisFile.atomicReplace(target: FortisFile) {
    withContext(Dispatchers.Default) {
        if (rename(posixPath(), target.posixPath()) < 0) throw posixError("rename")
    }
}

actual suspend fun FortisFile.syncDirectory() =
    withPosixFile(O_RDONLY or O_DIRECTORY) { syncPosix(it) }

actual suspend fun FortisFile.deleteIfExists() {
    withContext(Dispatchers.Default) {
        if (unlink(posixPath()) < 0 && errno != ENOENT) throw posixError("unlink")
    }
}
