package net.kigawa.fortis.raft.sharding

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

// 2レンジ構成でグループ毎の独立性を検証する
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
                "g1" to GroupHandle("g1", MemoryMvccStorageEngine()),
                "g2" to GroupHandle("g2", MemoryMvccStorageEngine()),
            )
        )
    }

    @Test
    fun putGoesToOwningGroupOnly() = runTest {
        val router = router()
        val keyA = byteArrayOf(0x61) // 'a' -> g1
        val keyM = byteArrayOf(0x6D) // 'm' -> g2

        router.put(keyA, byteArrayOf(1))
        router.put(keyM, byteArrayOf(2))

        // 正しいグループに届いている
        assertContentEquals(byteArrayOf(1), router.groups.getValue("g1").storage.get(keyA))
        assertContentEquals(byteArrayOf(2), router.groups.getValue("g2").storage.get(keyM))
        // 他グループには見えない
        assertNull(router.groups.getValue("g2").storage.get(keyA))
        assertNull(router.groups.getValue("g1").storage.get(keyM))
    }

    @Test
    fun snapshotReadsAreIndependentPerGroup() = runTest {
        val router = router()
        val keyA = byteArrayOf(0x61)
        val keyM = byteArrayOf(0x6D)

        // グループローカルな version 系列で適用する
        router.applyAt("g1", RaftCommand.Put(keyA, byteArrayOf(10)), 1)
        router.applyAt("g1", RaftCommand.Put(keyA, byteArrayOf(11)), 2)
        router.applyAt("g2", RaftCommand.Put(keyM, byteArrayOf(20)), 1)

        assertContentEquals(byteArrayOf(10), router.getAt(keyA, 1))
        assertContentEquals(byteArrayOf(11), router.getAt(keyA, 2))
        // g2 の version 1 が g1 の読みに混ざらない
        assertContentEquals(byteArrayOf(20), router.getAt(keyM, 1))
        assertEquals(2L, router.groups.getValue("g1").storage.latestVersion())
        assertEquals(1L, router.groups.getValue("g2").storage.latestVersion())
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
                mapOf("g1" to GroupHandle("g1", MemoryMvccStorageEngine())),
            )
        }
    }
}
