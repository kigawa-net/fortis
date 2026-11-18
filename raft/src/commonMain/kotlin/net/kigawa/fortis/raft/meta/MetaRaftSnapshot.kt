package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.RangeId
import net.kigawa.fortis.raft.sharding.RangeMetadata

enum class RegisteredNodeState { ACTIVE, DRAINING, OFFLINE }

data class RegisteredNode(
    val nodeId: String,
    val address: String,
    val state: RegisteredNodeState,
    val lastHeartbeat: Long,
) {
    init {
        require(nodeId.isNotBlank() && address.isNotBlank())
        require(lastHeartbeat >= 0)
    }
}

/** コミット済みの構成、ノード登録情報、レンジ一覧。membership が null の間は初期化前。 */
data class MetaRaftSnapshot(
    val membership: ClusterMembership?,
    val nodes: Map<String, RegisteredNode>,
    val ranges: Map<RangeId, RangeMetadata>,
    val pendingPromotion: String? = null,
)
