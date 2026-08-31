package net.kigawa.fortis.io.fs


actual suspend fun Fs.openRead(
    file: FortisFile, block: suspend (input: FsInput) -> Unit,
) {
    TODO()
}


actual suspend fun Fs.openWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (output: FsOutput) -> Unit,
) {
}

actual suspend fun Fs.openReadWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (io: FsIo) -> Unit,
) {
}
