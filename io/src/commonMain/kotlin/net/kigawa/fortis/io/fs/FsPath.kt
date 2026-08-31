package net.kigawa.fortis.io.fs

data class FsPath(
    val elements: List<Element>,
    val isAbsolute: Boolean,
) {
    sealed interface Element {
        data class Name(val name: String): Element {
            override fun toString(): String {
                return name
            }
        }

        object Parent: Element {
            override fun toString(): String {
                return ".."
            }
        }
    }

    fun toFile(): FortisFile = Fs.getFile(this)
    override fun toString(): String {
        return elements.joinToString("/", prefix = if (isAbsolute) "/" else "") { it.toString() }
    }
}