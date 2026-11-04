package net.kigawa.fortis.storage.engine

// 符号なしバイトの辞書順。接頭辞同士では短い方が小さい。キー範囲スキャンの順序に使う。
object BytewiseOrder : Comparator<ByteArray> {
    override fun compare(a: ByteArray, b: ByteArray): Int {
        val common = minOf(a.size, b.size)
        for (i in 0 until common) {
            val x = a[i].toInt() and 0xFF
            val y = b[i].toInt() and 0xFF
            if (x != y) return x - y
        }
        return a.size - b.size
    }
}

fun ByteArray.hasBytePrefix(prefix: ByteArray): Boolean {
    if (prefix.size > size) return false
    for (i in prefix.indices) {
        if (this[i] != prefix[i]) return false
    }
    return true
}
