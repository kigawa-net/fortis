package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.RangeTable

/**
 * 単一レンジの移動指示。実データ移動は行わず計画の算出に留める。
 *
 * [fromMemberId] は現在の担当、[toMemberId] は目標の担当である。
 */
data class RangeMove(
    val rangeId: String,
    val fromMemberId: String,
    val toMemberId: String,
)

/**
 * 配置計画の算出器。実データ移動は行わない。
 */
object RebalancePlanner {
    /**
     * 現在配置と目標配置の差分から移動リストを算出する。
     *
     * 担当が変わったレンジのみを、rangeId 昇順で返す。
     */
    fun diff(current: RangePlacement, desired: RangePlacement): List<RangeMove> {
        val moves = ArrayList<RangeMove>()
        for (rangeId in current.assignment.keys.sorted()) {
            val from = current.assignment.getValue(rangeId)
            val to = checkNotNull(desired.assignment[rangeId]) {
                "no desired placement for range $rangeId"
            }
            if (from != to) moves.add(RangeMove(rangeId, from, to))
        }
        return moves
    }

    /**
     * 全レンジを配置対象構成員へ均等割り付けした目標配置との差分を算出する。
     *
     * 対象は離脱予告中 ([ClusterMemberStatus.LEAVING]) を除く構成員とし、
     * rangeId 昇順・memberId 昇順で決定的に割り付ける。
     * 余りは先頭の構成員から 1 つずつ多く受け持つ。
     * 現在配置は [rangeTable] の全レンジを覆っていること。
     */
    fun planEvenDistribution(
        rangeTable: RangeTable,
        membership: ClusterMembership,
        current: RangePlacement,
    ): List<RangeMove> {
        val rangeIds = rangeTable.ranges.map { it.rangeId }.sorted()
        require(current.assignment.keys == rangeIds.toSet()) {
            "current placement must cover exactly the table ranges"
        }
        val targets = membership.placeableMembers().sorted()
        require(targets.isNotEmpty()) { "no placeable members" }
        val desired = rangeIds.mapIndexed { index, rangeId ->
            rangeId to targets[slotOwner(index, rangeIds.size, targets.size)]
        }.toMap()
        return diff(current, RangePlacement(desired))
    }

    /**
     * 均等割り付けの目標配置そのものを算出する。
     *
     * 実データ移動は行わず、[MetaStore.updatePlacement] への入力用途を想定する。
     */
    fun evenDistribution(
        rangeTable: RangeTable,
        membership: ClusterMembership,
    ): RangePlacement {
        val rangeIds = rangeTable.ranges.map { it.rangeId }.sorted()
        val targets = membership.placeableMembers().sorted()
        require(targets.isNotEmpty()) { "no placeable members" }
        val desired = rangeIds.mapIndexed { index, rangeId ->
            rangeId to targets[slotOwner(index, rangeIds.size, targets.size)]
        }.toMap()
        return RangePlacement(desired)
    }

    /**
     * 均等割り付け後の [rangeId] の担当 memberId を返す。配置の検証用途。
     *
     * 割り付け規則は [planEvenDistribution] と同一である。
     */
    fun ownerAfterEvenDistribution(
        rangeTable: RangeTable,
        membership: ClusterMembership,
        rangeId: String,
    ): String = evenDistribution(rangeTable, membership).ownerOf(rangeId)

    private fun slotOwner(index: Int, rangeCount: Int, memberCount: Int): Int {
        val base = rangeCount / memberCount
        val remainder = rangeCount % memberCount
        // 余り分を先頭メンバーに配る: 先頭 remainder 人が base+1 個受け持つ。
        val wideSlots = remainder * (base + 1)
        if (index < wideSlots) return index / (base + 1)
        return remainder + (index - wideSlots) / base
    }
}
