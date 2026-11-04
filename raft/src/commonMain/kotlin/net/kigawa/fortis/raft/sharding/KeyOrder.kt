package net.kigawa.fortis.raft.sharding

/**
 * キーの符号なし辞書式順序の比較ヘルパー。
 *
 * 先頭バイトから順に符号なし値（0..255）で比較し、
 * 共通 prefix が等しい場合は短い方を小さいとする。
 */
object KeyOrder {
    /** [a] と [b] を符号なし辞書式順序で比較する。 */
    fun compare(a: ByteArray, b: ByteArray): Int {
        val common = minOf(a.size, b.size)
        for (i in 0 until common) {
            val diff = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
            if (diff != 0) return if (diff < 0) -1 else 1
        }
        return a.size.compareTo(b.size)
    }
}
