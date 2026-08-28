package net.kigawa.fortis.io.fs


object Fs {
    fun getPath(strPath: String): FsPath = getPath(
        strPath.split("/"), strPath.startsWith("/")
    )

    fun getPath(strElements: List<String>, isAbsolute: Boolean): FsPath = getPath(
        strElements
            .map { if (it == "..") FsPath.Element.Parent else FsPath.Element.Name(it) },
        isAbsolute
    )

    fun getPath(elements: List<FsPath.Element>, isAbsolute: Boolean): FsPath =
        FsPath(elements, isAbsolute)

    fun getFile(path: FsPath): FortisFile = FortisFile(path)
}