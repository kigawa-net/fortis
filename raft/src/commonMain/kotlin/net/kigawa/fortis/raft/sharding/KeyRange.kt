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
    // 内部状態は外部に公開しない。getter は毎回コピーを返す。
    private val startValue: ByteArray? = startInclusive?.copyOf()
    private val endValue: ByteArray? = endExclusive?.copyOf()

    val startInclusive: ByteArray?
        get() = startValue?.copyOf()
    val endExclusive: ByteArray?
        get() = endValue?.copyOf()

    init {
        require(rangeId.isNotEmpty()) { "rangeId must not be empty" }
        val start = startValue
        val end = endValue
        if (start != null && end != null) {
            require(KeyOrder.compare(start, end) < 0) {
                "empty range: start must be smaller than end (rangeId=$rangeId)"
            }
        }
    }

    /** [key] がこのレンジに属するかを返す。 */
    fun contains(key: ByteArray): Boolean {
        val start = startValue
        if (start != null && KeyOrder.compare(key, start) < 0) return false
        val end = endValue
        if (end != null && KeyOrder.compare(key, end) >= 0) return false
        return true
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is KeyRange) return false
        if (rangeId != other.rangeId) return false
        if (!startValue.contentEqualsNullable(other.startValue)) return false
        if (!endValue.contentEqualsNullable(other.endValue)) return false
        return true
    }

    override fun hashCode(): Int {
        var result = rangeId.hashCode()
        result = 31 * result + (startValue?.contentHashCode() ?: 0)
        result = 31 * result + (endValue?.contentHashCode() ?: 0)
        return result
    }

    override fun toString(): String =
        "KeyRange(rangeId='$rangeId')"

    private fun ByteArray?.contentEqualsNullable(other: ByteArray?): Boolean {
        if (this == null || other == null) return this == null && other == null
        return contentEquals(other)
    }
}
