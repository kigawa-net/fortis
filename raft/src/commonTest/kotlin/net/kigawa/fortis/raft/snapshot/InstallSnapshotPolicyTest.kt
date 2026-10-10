package net.kigawa.fortis.raft.snapshot

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

// リーダー側の送信判断ヘルパーを確認する
class InstallSnapshotPolicyTest {
    @Test
    fun sendsSnapshotWhenNextIndexIsCompactedAway() {
        assertTrue(InstallSnapshotPolicy.shouldSendSnapshot(1, 3))
        assertTrue(InstallSnapshotPolicy.shouldSendSnapshot(3, 3))
        assertFalse(InstallSnapshotPolicy.shouldSendSnapshot(4, 3))
    }

    @Test
    fun rejectsNonPositiveInputs() {
        assertFailsWith<IllegalArgumentException> {
            InstallSnapshotPolicy.shouldSendSnapshot(0, 3)
        }
        assertFailsWith<IllegalArgumentException> {
            InstallSnapshotPolicy.shouldSendSnapshot(1, 0)
        }
    }

    @Test
    fun nextIndexFollowsSnapshotBoundary() {
        assertEquals(
            4,
            InstallSnapshotPolicy.nextIndexAfterSnapshot(SnapshotMetadata(3, 2)),
        )
    }
}
