package net.kigawa.fortis.raft.meta

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class ClusterMembershipTest {
    @Test
    fun joinAdvancesVersion() {
        val membership = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        assertEquals(0, membership.version)
        val next = membership.join(ClusterMember("m3"))
        assertEquals(1, next.version)
        assertEquals(setOf("m1", "m2", "m3"), next.members.keys)
    }

    @Test
    fun leaveAdvancesVersion() {
        val membership = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        val next = membership.leave("m2")
        assertEquals(1, next.version)
        assertEquals(setOf("m1"), next.members.keys)
    }

    @Test
    fun rejectsVoterElimination() {
        // 学習者のみの初期構成自体が拒否されること
        assertFailsWith<IllegalArgumentException> {
            ClusterMembership.initial(
                listOf(ClusterMember("m1", ClusterMemberRole.LEARNER))
            )
        }
        // 単一投票者の離脱は拒否されること
        val single = ClusterMembership.initial(listOf(ClusterMember("m1")))
        assertFailsWith<IllegalArgumentException> {
            single.leave("m1")
        }
        // 投票者を全て除去する置換も拒否されること
        val membership = ClusterMembership.initial(
            listOf(ClusterMember("m1"), ClusterMember("m2"))
        )
        assertFailsWith<IllegalArgumentException> {
            membership.join(ClusterMember("m1", ClusterMemberRole.LEARNER))
                .leave("m2")
        }
    }

    @Test
    fun rejectsUnknownMemberLeave() {
        val membership = ClusterMembership.initial(listOf(ClusterMember("m1")))
        assertFailsWith<IllegalArgumentException> {
            membership.leave("unknown")
        }
    }
}
