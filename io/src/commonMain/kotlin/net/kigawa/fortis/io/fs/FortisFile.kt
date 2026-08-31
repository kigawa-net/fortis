package net.kigawa.fortis.io.fs

data class FortisFile(val path: FsPath) {
}

expect suspend fun FortisFile.openRead(block: suspend (input: FsInput) -> Unit)

expect suspend fun FortisFile.openWrite(isCreate: Boolean, block: suspend (output: FsOutput) -> Unit)

expect suspend fun FortisFile.openReadWrite(isCreate: Boolean, block: suspend (io: FsIo) -> Unit)
