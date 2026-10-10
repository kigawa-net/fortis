package net.kigawa.fortis.raft.sharding

import net.kigawa.fortis.raft.RaftCommand
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

// ルーターはルーティング計算専用であり、ストレージ I/O を行わない。
// 書き込みは RaftRuntime.propose、読み取りは linearizableRead 経由とする（#23 以降）。
class MultiRaftRouterTest {
    private fun router(): MultiRaftRouter {
        val table = RangeTable(
            listOf(
                KeyRange("g1", null, byteArrayOf(0x6D)),
                KeyRange("g2", byteArrayOf(0x6D), null),
            )
        )
        return MultiRaftRouter(
            table,
            mapOf(
                "g1" to GroupHandle("g1"),
                "g2" to GroupHandle("g2"),
            )
        )
    }

    @Test
    fun routeResolvesOwningGroup() {
        val router = router()
        assertEquals("g1", router.route(byteArrayOf(0x61)).rangeId)
        assertEquals("g2", router.route(byteArrayOf(0x6D)).rangeId)
        assertEquals("g1", router.rangeIdOf(byteArrayOf(0x61)))
        assertEquals("g2", router.rangeIdOf(byteArrayOf(0x6D)))
    }

    @Test
    fun routeCommandResolvesOwningGroup() {
        val router = router()
        assertEquals("g1", router.routeCommand(RaftCommand.Put(byteArrayOf(0x61), byteArrayOf(1))).rangeId)
        assertEquals("g2", router.routeCommand(RaftCommand.Delete(byteArrayOf(0x6D))).rangeId)
    }

    @Test
    fun rejectsGroupMismatch() {
        val table = RangeTable(
            listOf(
                KeyRange("g1", null, byteArrayOf(0x6D)),
                KeyRange("g2", byteArrayOf(0x6D), null),
            )
        )
        assertFailsWith<IllegalArgumentException> {
            MultiRaftRouter(
                table,
                mapOf("g1" to GroupHandle("g1")),
            )
        }
    }
}
