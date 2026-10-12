package net.kigawa.fortis.raft.meta

/**
 * クラスタ構成員の役割。
 *
 * [VOTER] はリーダー選出とログ合意に参加する。[LEARNER] はログを受け取るのみで投票しない。
 */
enum class ClusterMemberRole {
    VOTER,
    LEARNER,
}

/**
 * クラスタ構成員の状態。
 *
 * [ACTIVE] は通常参加、[JOINING] は参加途中、[LEAVING] は離脱予告中を表す。
 */
enum class ClusterMemberStatus {
    ACTIVE,
    JOINING,
    LEAVING,
}

/**
 * クラスタの単一構成員。
 *
 * [memberId] は空でない一意な識別子であること。
 */
data class ClusterMember(
    val memberId: String,
    val role: ClusterMemberRole = ClusterMemberRole.VOTER,
    val status: ClusterMemberStatus = ClusterMemberStatus.ACTIVE,
) {
    init {
        require(memberId.isNotEmpty()) { "memberId must not be empty" }
    }

    /** 投票権を持つ構成員かを返す。状態には依存しない。 */
    fun isVoter(): Boolean = role == ClusterMemberRole.VOTER

    /** 離脱予告中でなくレンジ配置の対象になり得るかを返す。 */
    fun isPlaceable(): Boolean = status != ClusterMemberStatus.LEAVING
}
