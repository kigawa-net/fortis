package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeTable
import kotlin.test.Test
import kotlin.test.assertFailsWith

class RangePlacementTest {
    private fun table(): RangeTable = RangeTable(
        listOf(
            KeyRange("r1", null, byteArrayOf(0x6D)),
            KeyRange("r2", byteArrayOf(0x6D), null),
        )
    )

    private fun membership(): ClusterMembership = ClusterMembership.initial(
        listOf(ClusterMember("m1"), ClusterMember("m2"))
    )

    @Test
    fun acceptsConsistentPlacement() {
        RangePlacement(mapOf("r1" to "m1", "r2" to "m2")).validate(table(), membership())
    }

    @Test
    fun rejectsUnknownRangeId() {
        assertFailsWith<IllegalArgumentException> {
            RangePlacement(mapOf("r1" to "m1", "rX" to "m2")).validate(table(), membership())
        }
    }

    @Test
    fun rejectsMissingRangeId() {
        assertFailsWith<IllegalArgumentException> {
            RangePlacement(mapOf("r1" to "m1")).validate(table(), membership())
        }
    }

    @Test
    fun rejectsUnknownMember() {
        assertFailsWith<IllegalArgumentException> {
            RangePlacement(mapOf("r1" to "m1", "r2" to "mX")).validate(table(), membership())
        }
    }
}
