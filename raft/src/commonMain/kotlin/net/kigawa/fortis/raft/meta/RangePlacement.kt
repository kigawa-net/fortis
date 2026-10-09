package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.RangeTable

/**
 * レンジから担当構成員への配置。
 *
 * ルーティング計算のための値であり、ストレージ I/O や実データ移動は行わない。
 * キーは rangeId、値は担当の memberId である。
 */
class RangePlacement(
    assignment: Map<String, String>,
) {
    /** rangeId から担当 memberId への対応。 */
    val assignment: Map<String, String> = assignment.toMap()

    init {
        require(assignment.isNotEmpty()) { "assignment must not be empty" }
        for ((rangeId, memberId) in assignment) {
            require(rangeId.isNotEmpty()) { "rangeId must not be empty" }
            require(memberId.isNotEmpty()) { "memberId must not be empty (rangeId=$rangeId)" }
        }
    }

    /** [rangeId] の担当 memberId を返す。未知の場合は例外を投げる。 */
    fun ownerOf(rangeId: String): String =
        checkNotNull(assignment[rangeId]) { "no placement for range $rangeId" }

    /**
     * [rangeTable] の rangeId 集合と [membership] の構成員集合に対する整合性を検証する。
     *
     * 配置が全レンジを過不足なく覆い、担当がすべて既知の構成員であることを要求する。
     * 不整合があれば [IllegalArgumentException] を投げる。
     */
    fun validate(rangeTable: RangeTable, membership: ClusterMembership) {
        val tableIds = rangeTable.ranges.map { it.rangeId }.toSet()
        require(assignment.keys == tableIds) {
            "placement must cover exactly the table ranges: table=$tableIds placement=${assignment.keys}"
        }
        val memberIds = membership.members.keys
        for ((rangeId, memberId) in assignment) {
            require(memberIds.contains(memberId)) {
                "unknown member in placement: range=$rangeId member=$memberId"
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangePlacement) return false
        return assignment == other.assignment
    }

    override fun hashCode(): Int = assignment.hashCode()

    override fun toString(): String = "RangePlacement(assignment=$assignment)"
}
