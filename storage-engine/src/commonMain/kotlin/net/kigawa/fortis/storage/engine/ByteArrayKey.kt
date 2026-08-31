package net.kigawa.fortis.storage.engine

class ByteArrayKey(
    val bytes: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        return other is ByteArrayKey && bytes.contentEquals(other.bytes)
    }

    override fun hashCode(): Int {
        return bytes.contentHashCode()
    }
}