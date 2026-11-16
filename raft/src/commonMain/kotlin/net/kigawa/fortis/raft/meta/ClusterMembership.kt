package net.kigawa.fortis.raft.meta

/**
 * 単調増加の版付きで不変なクラスタ構成。
 *
 * [version] は構成変更の順序付けに使う単調増加値であり、[join]・[leave] で +1 される。
 * 投票者 ([ClusterMember.isVoter]) が 0 の構成は作れない。
 */
class ClusterMembership(
    val version: Long,
    members: Map<String, ClusterMember>,
) {
    /** 構成員一覧。キーは [ClusterMember.memberId] と一致する。 */
    val members: Map<String, ClusterMember> = members.toMap()

    init {
        require(version >= 0) { "version must be >= 0, but was $version" }
        require(members.isNotEmpty()) { "members must not be empty" }
        for ((id, member) in members) {
            require(id == member.memberId) {
                "membership key mismatch: key=$id member=${member.memberId}"
            }
        }
        require(members.values.any { it.isVoter() }) {
            "membership must contain at least one voter"
        }
    }

    /** 投票者の識別子一覧を返す。 */
    fun voters(): Set<String> = members.values.filter { it.isVoter() }.map { it.memberId }.toSet()

    /** 配置対象になり得る構成員 ([ClusterMember.isPlaceable]) の識別子一覧を返す。 */
    fun placeableMembers(): Set<String> =
        members.values.filter { it.isPlaceable() }.map { it.memberId }.toSet()

    /**
     * [member] を追加・置換した版+1 の新構成を返す。
     *
     * 結果として投票者が 0 になる場合は拒否する。
     */
    fun join(member: ClusterMember): ClusterMembership {
        val next = members + (member.memberId to member)
        require(next.values.any { it.isVoter() }) {
            "join refused: membership must contain at least one voter"
        }
        return ClusterMembership(version + 1, next)
    }

    /**
     * [memberId] を除去した版+1 の新構成を返す。
     *
     * 未知の [memberId] や、結果として投票者が 0 になる変更は拒否する。
     */
    fun leave(memberId: String): ClusterMembership {
        require(members.containsKey(memberId)) { "unknown member: $memberId" }
        val next = members - memberId
        require(next.isNotEmpty()) { "leave refused: membership must not be empty" }
        require(next.values.any { it.isVoter() }) {
            "leave refused: membership must contain at least one voter"
        }
        return ClusterMembership(version + 1, next)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ClusterMembership) return false
        return version == other.version && members == other.members
    }

    override fun hashCode(): Int = 31 * version.hashCode() + members.hashCode()

    override fun toString(): String = "ClusterMembership(version=$version, members=${members.keys})"

    companion object {
        /**
         * 初期構成を作る。版は 0 から始まる。
         *
         * 空集合や投票者なしは拒否する。
         */
        fun initial(members: Collection<ClusterMember>): ClusterMembership {
            return ClusterMembership(0, members.associateBy { it.memberId })
        }
    }
}
