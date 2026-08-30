package net.kigawa.fortis.io.fs

data class FortisFile(val path: FsPath) {

    suspend fun openRead(block: suspend (input: FsInput) -> Unit) = Fs.openRead(this, block)

    suspend fun openWrite(block: suspend (input: FsOutput) -> Unit) = Fs.openWrite(this, block)

    suspend fun openReadWrite(block: suspend (input: FsIo) -> Unit) = Fs.openReadWrite(this, block)
}