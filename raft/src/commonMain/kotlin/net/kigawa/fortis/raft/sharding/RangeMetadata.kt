package net.kigawa.fortis.raft.sharding

import kotlin.jvm.JvmInline
@JvmInline
value class RangeId(val value: String) {
    init {
        require(value.isNotEmpty()) { "range ID must not be empty" }
    }
}

enum class RangeState { ACTIVE, SPLITTING, DELETED }

/** レンジの半開区間と、要求を検証するための世代を保持する。 */
class RangeMetadata(
    val keyRange: KeyRange,
    val epoch: Long = 0,
    val state: RangeState = RangeState.ACTIVE,
) {
    val rangeId = RangeId(keyRange.rangeId)

    init {
        require(epoch >= 0) { "epoch must not be negative" }
    }

    /** 分割の開始時に世代を進め、親レンジへのクライアント要求を停止する。 */
    fun beginSplit(): RangeMetadata {
        check(state == RangeState.ACTIVE)
        return RangeMetadata(keyRange, nextEpoch(), RangeState.SPLITTING)
    }

    /** 分割中の親から、境界を共有する二つの新しいレンジを作る。 */
    fun splitAt(key: ByteArray, left: RangeId, right: RangeId): Pair<RangeMetadata, RangeMetadata> {
        check(state == RangeState.SPLITTING)
        require(left != right && left != rangeId && right != rangeId) { "split requires fresh range IDs" }
        val start = keyRange.startInclusive
        require(keyRange.contains(key) && KeyOrder.compare(start ?: byteArrayOf(), key) < 0) {
            "split key must be inside the range"
        }
        val epoch = nextEpoch()
        return RangeMetadata(KeyRange(left.value, start, key), epoch) to
            RangeMetadata(KeyRange(right.value, key, keyRange.endExclusive), epoch)
    }

    /** 再配置の切り替え時に世代を進める。 */
    fun relocated(): RangeMetadata {
        check(state == RangeState.ACTIVE)
        return RangeMetadata(keyRange, nextEpoch())
    }

    /** レンジを廃止して、同じ ID の再利用を防ぐ墓標を作る。 */
    fun deleted(): RangeMetadata {
        check(state != RangeState.DELETED)
        return RangeMetadata(keyRange, nextEpoch(), RangeState.DELETED)
    }

    private fun nextEpoch(): Long {
        check(epoch < Long.MAX_VALUE) { "range epoch is exhausted" }
        return epoch + 1
    }
}

/** クライアントが解決したグループとレンジの世代。 */
data class RangeRoute(val group: GroupHandle, val epoch: Long) {
    val rangeId = RangeId(group.rangeId)

    init {
        require(epoch >= 0) { "epoch must not be negative" }
    }
}

class StaleRangeEpochException(val rangeId: RangeId, val supplied: Long, val current: Long) :
    IllegalStateException("Range ${rangeId.value} epoch mismatch: supplied=$supplied current=$current")

class RangeUnavailableException(val rangeId: RangeId) :
    IllegalStateException("Range ${rangeId.value} is unavailable")
