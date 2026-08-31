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

actual suspend fun Fs.readAt(
    input: FsInput, offset: FsOffset, buffer: ByteArray,
): Int {
    TODO("Not yet implemented")
}

actual suspend fun Fs.writeAt(
    output: FsOutput, offset: FsOffset, data: ByteArray,
) : Int{
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