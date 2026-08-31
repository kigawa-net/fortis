package net.kigawa.fortis.io.fs


actual suspend fun Fs.openRead(
    file: FortisFile, block: suspend (input: FsInput) -> Unit,
) {
}

actual suspend fun Fs.openWrite(
    file: FortisFile, block: suspend (output: FsOutput) -> Unit,
) {
}

actual suspend fun Fs.openReadWrite(
    file: FortisFile, block: suspend (io: FsIo) -> Unit,
) {
}