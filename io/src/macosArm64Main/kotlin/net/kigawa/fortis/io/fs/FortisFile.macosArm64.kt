package net.kigawa.fortis.io.fs

actual suspend fun FortisFile.openRead(
    block: suspend (input: FsInput) -> Unit,
) {
}

actual suspend fun FortisFile.openWrite(
    isCreate: Boolean,
    block: suspend (output: FsOutput) -> Unit,
) {
}

actual suspend fun FortisFile.openReadWrite(
    isCreate: Boolean,
    block: suspend (io: FsIo) -> Unit,
) {
}