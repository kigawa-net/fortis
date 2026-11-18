package net.kigawa.fortis.raft.sharding

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RangeMetadataTest {
    private val active = RangeMetadata(KeyRange("parent", null, null))
    private val splitKey = byteArrayOf(10)
    private val left = RangeId("left")
    private val right = RangeId("right")

    @Test
    fun splitAndDeleteAdvanceEpochAndKeepEarlierMetadata() {
        val splitting = active.beginSplit()
        val deleted = splitting.deleted()

        assertEquals(RangeState.ACTIVE, active.state)
        assertEquals(0L, active.epoch)
        assertEquals(RangeState.SPLITTING, splitting.state)
        assertEquals(1L, splitting.epoch)
        assertEquals(RangeState.DELETED, deleted.state)
        assertEquals(2L, deleted.epoch)
        assertEquals(active.rangeId, splitting.rangeId)
        assertEquals(active.rangeId, deleted.rangeId)
    }

    @Test
    fun splittingAndDeletedMetadataRejectInvalidTransitions() {
        val splitting = active.beginSplit()
        val deleted = splitting.deleted()

        assertFailsWith<IllegalStateException> { active.splitAt(splitKey, left, right) }
        assertFailsWith<IllegalStateException> { splitting.beginSplit() }
        assertFailsWith<IllegalStateException> { splitting.relocated() }
        assertFailsWith<IllegalStateException> { deleted.beginSplit() }
        assertFailsWith<IllegalStateException> { deleted.splitAt(splitKey, left, right) }
        assertFailsWith<IllegalStateException> { deleted.relocated() }
        assertFailsWith<IllegalStateException> { deleted.deleted() }
    }
}
