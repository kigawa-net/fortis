package net.kigawa.fortis.raft.sharding

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// RangeTable の境界・検証ふるまいを確認する
class RangeTableTest {
    @Test
    fun singleRangeCoversAll() {
        val table = RangeTable(listOf(KeyRange("all", null, null)))
        assertEquals("all", table.route(byteArrayOf()).rangeId)
        assertEquals("all", table.route(byteArrayOf(0x00)).rangeId)
        assertEquals("all", table.route(byteArrayOf(0x7F, 0x10)).rangeId)
    }

    @Test
    fun boundaryIsStartInclusiveEndExclusive() {
        val table = RangeTable(
            listOf(
                KeyRange("a", null, byteArrayOf(0x6D)),
                KeyRange("b", byteArrayOf(0x6D), null),
            )
        )
        // end 側は含まず次レンジへ
        assertEquals("a", table.route(byteArrayOf(0x6C)).rangeId)
        assertEquals("b", table.route(byteArrayOf(0x6D)).rangeId)
        assertEquals("b", table.route(byteArrayOf(0x6E)).rangeId)
    }

    @Test
    fun multipleRangesRouteByBinarySearch() {
        val table = RangeTable(
            listOf(
                KeyRange("r1", null, byteArrayOf(0x10)),
                KeyRange("r2", byteArrayOf(0x10), byteArrayOf(0x20)),
                KeyRange("r3", byteArrayOf(0x20), null),
            )
        )
        assertEquals("r1", table.route(byteArrayOf(0x0F)).rangeId)
        assertEquals("r2", table.route(byteArrayOf(0x10)).rangeId)
        assertEquals("r2", table.route(byteArrayOf(0x1F)).rangeId)
        assertEquals("r3", table.route(byteArrayOf(0x20)).rangeId)
        assertEquals("r3", table.route(byteArrayOf(0x7F.toByte())).rangeId)
    }

    @Test
    fun unsignedByteOrder() {
        // 0xFF は符号付きでは -1 だが符号なしでは 255 として最大側
        val table = RangeTable(
            listOf(
                KeyRange("low", null, byteArrayOf(0x7F.toByte())),
                KeyRange("high", byteArrayOf(0x7F.toByte()), null),
            )
        )
        assertEquals("low", table.route(byteArrayOf(0x01)).rangeId)
        assertEquals("high", table.route(byteArrayOf(0xFF.toByte())).rangeId)
        assertTrue(KeyOrder.compare(byteArrayOf(0x7F.toByte()), byteArrayOf(0xFF.toByte())) < 0)
    }

    @Test
    fun rejectsEmptyRange() {
        assertFailsWith<IllegalArgumentException> {
            KeyRange("empty", byteArrayOf(0x10), byteArrayOf(0x10))
        }
        assertFailsWith<IllegalArgumentException> {
            KeyRange("inverted", byteArrayOf(0x20), byteArrayOf(0x10))
        }
    }

    @Test
    fun rejectsGap() {
        assertFailsWith<IllegalArgumentException> {
            RangeTable(
                listOf(
                    KeyRange("a", null, byteArrayOf(0x10)),
                    // 0x10..0x20 がカバーされない
                    KeyRange("b", byteArrayOf(0x20), null),
                )
            )
        }
    }

    @Test
    fun rejectsOverlap() {
        assertFailsWith<IllegalArgumentException> {
            RangeTable(
                listOf(
                    KeyRange("a", null, byteArrayOf(0x20)),
                    KeyRange("b", byteArrayOf(0x10), null),
                )
            )
        }
    }

    @Test
    fun rejectsDuplicateId() {
        assertFailsWith<IllegalArgumentException> {
            RangeTable(
                listOf(
                    KeyRange("same", null, byteArrayOf(0x10)),
                    KeyRange("same", byteArrayOf(0x10), null),
                )
            )
        }
    }

    @Test
    fun rejectsUncoveredHeadAndTail() {
        assertFailsWith<IllegalArgumentException> {
            RangeTable(listOf(KeyRange("only", byteArrayOf(0x00), null)))
        }
        assertFailsWith<IllegalArgumentException> {
            RangeTable(listOf(KeyRange("only", null, byteArrayOf(0x00))))
        }
    }
}
