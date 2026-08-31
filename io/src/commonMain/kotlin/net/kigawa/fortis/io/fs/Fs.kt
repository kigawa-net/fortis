package net.kigawa.fortis.io.fs

typealias FsOffset = Long
typealias FsByteSize = Long

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


}

expect suspend fun Fs.openRead(file: FortisFile, block: suspend (input: FsInput) -> Unit)

expect suspend fun Fs.openWrite(file: FortisFile, isCreate: Boolean, block: suspend (output: FsOutput) -> Unit)

expect suspend fun Fs.openReadWrite(file: FortisFile, isCreate: Boolean, block: suspend (io: FsIo) -> Unit)
