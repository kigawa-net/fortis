package net.kigawa.fortis.raft.sharding

/**
 * キー空間の半開区間 `[startInclusive, endExclusive)`。
 *
 * [startInclusive] が null の場合は先頭から、[endExclusive] が null の場合は
 * 上限なし（最終レンジ）を表す。空レンジ（start >= end）は受け付けない。
 */
class KeyRange(
    val rangeId: String,
    startInclusive: ByteArray?,
    endExclusive: ByteArray?,
) {
    val startInclusive: ByteArray? = startInclusive?.copyOf()
    val endExclusive: ByteArray? = endExclusive?.copyOf()

    init {
        require(rangeId.isNotEmpty()) { "rangeId must not be empty" }
        val start = this.startInclusive
        val end = this.endExclusive
        if (start != null && end != null) {
            require(KeyOrder.compare(start, end) < 0) {
                "empty range: start must be smaller than end (rangeId=$rangeId)"
            }
        }
    }

    /** [key] がこのレンジに属するかを返す。 */
    fun contains(key: ByteArray): Boolean {
        val start = startInclusive
        if (start != null && KeyOrder.compare(key, start) < 0) return false
        val end = endExclusive
        if (end != null && KeyOrder.compare(key, end) >= 0) return false
        return true
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is KeyRange) return false
        if (rangeId != other.rangeId) return false
        if (!startInclusive.contentEqualsNullable(other.startInclusive)) return false
        if (!endExclusive.contentEqualsNullable(other.endExclusive)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = rangeId.hashCode()
        result = 31 * result + (startInclusive?.contentHashCode() ?: 0)
        result = 31 * result + (endExclusive?.contentHashCode() ?: 0)
        return result
    }

    override fun toString(): String =
        "KeyRange(rangeId='$rangeId')"

    private fun ByteArray?.contentEqualsNullable(other: ByteArray?): Boolean {
        if (this == null || other == null) return this == null && other == null
        return contentEquals(other)
    }
}
