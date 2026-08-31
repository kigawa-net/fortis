package net.kigawa.fortis.io.fs

actual suspend fun FortisFile.openRead(
    file: FortisFile, block: suspend (input: FsInput) -> Unit,
) {
}

actual suspend fun FortisFile.openWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (output: FsOutput) -> Unit,
) {
}

actual suspend fun FortisFile.openReadWrite(
    file: FortisFile, isCreate: Boolean,
    block: suspend (io: FsIo) -> Unit,
) {
}