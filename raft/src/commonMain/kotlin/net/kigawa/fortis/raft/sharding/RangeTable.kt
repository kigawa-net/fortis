package net.kigawa.fortis.raft.sharding

/**
 * 静的なレンジ構成の不変テーブル。全域カバーを前提とする。
 *
 * 構築時にキー順へソートし、rangeId の重複・重なり・ギャップを検証する。
 * 先頭レンジの [KeyRange.startInclusive] は null、最終レンジの
 * [KeyRange.endExclusive] は null でなければならない。
 */
class RangeTable(
    ranges: List<KeyRange>,
) {
    /** キー順にソート済みのレンジ一覧。 */
    val ranges: List<KeyRange>

    init {
        require(ranges.isNotEmpty()) { "ranges must not be empty" }
        val ids = ranges.map { it.rangeId }
        require(ids.toSet().size == ids.size) { "duplicate rangeId: $ids" }

        val sorted = ranges.sortedWith { a, b ->
            when {
                a.startInclusive == null && b.startInclusive == null -> 0
                a.startInclusive == null -> -1
                b.startInclusive == null -> 1
                else -> {
                    val cmp = KeyOrder.compare(a.startInclusive, b.startInclusive)
                    if (cmp != 0) cmp else compareEnd(a.endExclusive, b.endExclusive)
                }
            }
        }
        require(sorted.first().startInclusive == null) {
            "first range must start with null (full coverage required)"
        }
        require(sorted.last().endExclusive == null) {
            "last range must end with null (full coverage required)"
        }
        for (i in 1 until sorted.size) {
            val prev = sorted[i - 1]
            val curr = sorted[i]
            val currStart = curr.startInclusive
            require(currStart != null) {
                "overlapping ranges: ${prev.rangeId} and ${curr.rangeId}"
            }
            // 開始位置が単調増加でなければ重複
            val prevStart = prev.startInclusive
            if (prevStart != null) {
                require(KeyOrder.compare(prevStart, currStart) < 0) {
                    "overlapping ranges: ${prev.rangeId} and ${curr.rangeId}"
                }
            }
            // 前レンジの終端と次レンジの始端が一致しなければギャップまたは重なり
            val prevEnd = prev.endExclusive
            require(prevEnd != null && prevEnd.contentEquals(currStart)) {
                "gap or overlap between ${prev.rangeId} and ${curr.rangeId}"
            }
        }
        this.ranges = sorted
    }

    /**
     * [key] の所属レンジを二分探索で返す。
     *
     * 全域カバーが構築時に保証されるため、見つからない場合は内部エラーとして例外を投げる。
     */
    fun route(key: ByteArray): KeyRange {
        var low = 0
        var high = ranges.size - 1
        var candidate: KeyRange? = null
        while (low <= high) {
            val mid = (low + high) ushr 1
            val range = ranges[mid]
            val start = range.startInclusive
            if (start == null || KeyOrder.compare(start, key) <= 0) {
                candidate = range
                low = mid + 1
            } else {
                high = mid - 1
            }
        }
        val found = candidate
        if (found != null && found.contains(key)) return found
        // 全域カバー前提のため到達しないはず
        error("no range found for key (table is broken)")
    }

    private fun compareEnd(a: ByteArray?, b: ByteArray?): Int {
        if (a == null && b == null) return 0
        if (a == null) return 1
        if (b == null) return -1
        return KeyOrder.compare(a, b)
    }
}
