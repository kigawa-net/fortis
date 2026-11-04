package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeTable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RebalancePlannerTest {
    private fun table4(): RangeTable = RangeTable(
        listOf(
            KeyRange("r1", null, byteArrayOf(0x20)),
            KeyRange("r2", byteArrayOf(0x20), byteArrayOf(0x40)),
            KeyRange("r3", byteArrayOf(0x40), byteArrayOf(0x60)),
            KeyRange("r4", byteArrayOf(0x60), null),
        )
    )

    @Test
    fun evenDistributionSplitsTwoAndTwo() {
        val table = table4()
        val membership = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        val current = RangePlacement(
            mapOf("r1" to "m1", "r2" to "m1", "r3" to "m1", "r4" to "m1")
        )
        val moves = RebalancePlanner.planEvenDistribution(table, membership, current)
        assertEquals(
            listOf(
                RangeMove("r3", "m1", "m2"),
                RangeMove("r4", "m1", "m2"),
            ),
            moves,
        )
    }

    @Test
    fun addingMemberProducesMoves() {
        val table = table4()
        val before = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        // 2 メンバーで 2-2 に均等化した状態から開始する
        val balanced = RebalancePlanner.evenDistribution(table, before)
        assertEquals("m1", balanced.ownerOf("r1"))
        assertEquals("m1", balanced.ownerOf("r2"))
        assertEquals("m2", balanced.ownerOf("r3"))
        assertEquals("m2", balanced.ownerOf("r4"))

        val after = before.join(ClusterMember("m3"))
        val moves = RebalancePlanner.planEvenDistribution(table, after, balanced)
        // 4 レンジ 3 メンバーでは 2-1-1 が目標となり移動が発生すること
        assertTrue(moves.isNotEmpty())
        val applied = balanced.assignment.toMutableMap()
        for (move in moves) {
            assertEquals(applied[move.rangeId], move.fromMemberId)
            applied[move.rangeId] = move.toMemberId
        }
        assertEquals(RebalancePlanner.evenDistribution(table, after), RangePlacement(applied))
    }

    @Test
    fun alreadyBalancedProducesNoMoves() {
        val table = table4()
        val membership = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        val balanced = RebalancePlanner.evenDistribution(table, membership)
        assertTrue(RebalancePlanner.planEvenDistribution(table, membership, balanced).isEmpty())
    }
}
