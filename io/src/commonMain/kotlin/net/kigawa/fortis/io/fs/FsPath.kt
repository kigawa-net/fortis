package net.kigawa.fortis.io.fs

data class FsPath(
    val elements: List<Element>,
    val isAbsolute: Boolean,
) {
    sealed interface Element {
        data class Name(val name: String): Element
        object Parent: Element
    }

    fun toFile(): FortisFile = Fs.getFile(this)
}