package net.kigawa.fortis.io.fs

data class FortisFile(val path: FsPath) {

    suspend fun openRead(block: suspend (input: FsInput) -> Unit) {
        TODO()
    }

    suspend fun openWrite(block: suspend (input: FsOutput) -> Unit) {
        TODO()
    }

    suspend fun openReadWrite(block: suspend (input: FsIo) -> Unit) {
        TODO()
    }
}