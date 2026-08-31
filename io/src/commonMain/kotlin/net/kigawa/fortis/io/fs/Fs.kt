package net.kigawa.fortis.io.fs

typealias FsOffset = Long
typealias FsSize = Long

object Fs {
    fun getPath(strPath: String): FsPath = getPath(
        strPath.split("/"), strPath.startsWith("/")
    )

    @kotlin.jvm.JvmName("getPathFromStrings")
    fun getPath(strElements: List<String>, isAbsolute: Boolean): FsPath = getPath(
        strElements.map { if (it == "..") FsPath.Element.Parent else FsPath.Element.Name(it) }, isAbsolute
    )

    fun getPath(elements: List<FsPath.Element>, isAbsolute: Boolean): FsPath = FsPath(elements, isAbsolute)

    fun getFile(path: FsPath): FortisFile = FortisFile(path)

    suspend fun readAt(input: FsInput, offset: FsOffset, buffer: ByteArray): Int {
        TODO()
    }

    suspend fun writeAt(output: FsOutput, offset: FsOffset, data: ByteArray) {
        TODO()
    }

    suspend fun writeAppend(output: FsOutput, data: ByteArray): FsOffset {
        TODO()
    }

    suspend fun sync(output: FsOutput) {
        TODO()
    }

    suspend fun truncate(output: FsOutput, size: FsSize) {
        TODO()
    }
}

expect suspend fun Fs.openRead(file: FortisFile, block: suspend (input: FsInput) -> Unit)

expect suspend fun Fs.openWrite(file: FortisFile, block: suspend (output: FsOutput) -> Unit)

expect suspend fun Fs.openReadWrite(file: FortisFile, block: suspend (io: FsIo) -> Unit)