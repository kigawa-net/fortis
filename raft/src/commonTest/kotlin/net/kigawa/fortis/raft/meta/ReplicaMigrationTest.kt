package net.kigawa.fortis.raft.meta

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import net.kigawa.fortis.raft.sharding.RangeId

class ReplicaMigrationTest {
    @Test
    fun balancesProjectedLoadsAndAvoidsExistingReplicasAndIneligibleTargets() {
        val loads = mapOf("a" to 90L, "b" to 40L, "c" to 10L, "d" to 0L)
        val hot = RangeReplicaLoad(RangeId("hot"), setOf("a", "b"), 30)
        val warm = RangeReplicaLoad(RangeId("warm"), setOf("a", "c"), 10)
        val result = listOf(
            ReplicaMigration(hot.rangeId, "a", "c"),
            ReplicaMigration(warm.rangeId, "a", "b"),
        )
        assertEquals(result, RebalancePlanner.planReplicaMigrations(loads, listOf(warm, hot), setOf("a", "b", "c")))
        assertEquals(result, RebalancePlanner.planReplicaMigrations(loads, listOf(hot, warm), setOf("a", "b", "c")))
        assertEquals(90L, loads.getValue("a"))
    }

    @Test
    fun skipsMovesThatDoNotImproveBalanceAndRejectsInconsistentLoad() {
        val loads = mapOf("a" to 25L, "b" to 15L)
        val range = RangeReplicaLoad(RangeId("warm"), setOf("a"), 10)
        assertTrue(RebalancePlanner.planReplicaMigrations(loads, listOf(range)).isEmpty())
        assertFailsWith<IllegalArgumentException> {
            RebalancePlanner.planReplicaMigrations(loads, listOf(
                range,
                RangeReplicaLoad(RangeId("hot"), setOf("a"), 16),
            ))
        }
    }

    @Test
    fun breaksEqualLoadTiesByNodeIdAndSkipsRangesWithoutAnEligibleDestination() {
        val loads = mapOf("b" to 40L, "a" to 40L, "d" to 0L, "c" to 0L)
        val range = RangeReplicaLoad(RangeId("range"), setOf("b", "a"), 10)

        assertEquals(
            listOf(ReplicaMigration(range.rangeId, "a", "c")),
            RebalancePlanner.planReplicaMigrations(loads, listOf(range)),
        )
        assertTrue(RebalancePlanner.planReplicaMigrations(loads, listOf(range), range.replicas).isEmpty())
    }
}
