package net.kigawa.fortis.raft.meta

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeTable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MetaStoreTest {
    private fun table(): RangeTable = RangeTable(
        listOf(
            KeyRange("r1", null, byteArrayOf(0x6D)),
            KeyRange("r2", byteArrayOf(0x6D), null),
        )
    )

    @Test
    fun updatesAdvanceVersion() = runTest {
        val table = table()
        val store = MetaStore(
            table,
            ClusterMembership.initial(listOf(ClusterMember("m1"), ClusterMember("m2"))),
            RangePlacement(mapOf("r1" to "m1", "r2" to "m2")),
        )
        assertEquals(0, store.snapshot().version)

        val joined = store.snapshot().membership.join(ClusterMember("m3"))
        val afterJoin = store.updateMembership(joined)
        assertEquals(1, afterJoin.version)
        assertEquals(setOf("m1", "m2", "m3"), afterJoin.membership.members.keys)

        val afterPlacement = store.updatePlacement(
            RangePlacement(mapOf("r1" to "m3", "r2" to "m2"))
        )
        assertEquals(afterJoin.version + 1, afterPlacement.version)
        assertEquals("m3", afterPlacement.placement.ownerOf("r1"))
    }

    @Test
    fun rejectsStaleMembership() = runTest {
        val table = table()
        val store = MetaStore(
            table,
            ClusterMembership.initial(listOf(ClusterMember("m1"), ClusterMember("m2"))),
            RangePlacement(mapOf("r1" to "m1", "r2" to "m2")),
        )
        val fresh = store.snapshot().membership.join(ClusterMember("m3"))
        store.updateMembership(fresh)
        assertFailsWith<IllegalArgumentException> {
            store.updateMembership(fresh)
        }
    }

    @Test
    fun rejectsPlacementLosingMember() = runTest {
        val table = table()
        val store = MetaStore(
            table,
            ClusterMembership.initial(listOf(ClusterMember("m1"), ClusterMember("m2"))),
            RangePlacement(mapOf("r1" to "m1", "r2" to "m2")),
        )
        // r1 の担当 m1 が消える構成変更は拒否されること
        assertFailsWith<IllegalArgumentException> {
            store.updateMembership(store.snapshot().membership.leave("m1"))
        }
    }

    @Test
    fun rejectsInconsistentPlacement() = runTest {
        val table = table()
        val store = MetaStore(
            table,
            ClusterMembership.initial(listOf(ClusterMember("m1"), ClusterMember("m2"))),
            RangePlacement(mapOf("r1" to "m1", "r2" to "m2")),
        )
        assertFailsWith<IllegalArgumentException> {
            store.updatePlacement(RangePlacement(mapOf("r1" to "m1", "r2" to "mX")))
        }
    }
}
