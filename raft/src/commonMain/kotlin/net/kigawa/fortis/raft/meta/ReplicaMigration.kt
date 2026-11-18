package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.RangeId

/** レンジの既存レプリカと、レプリカ 1 個の負荷。 */
data class RangeReplicaLoad(
    val rangeId: RangeId,
    val replicas: Set<String>,
    val loadPerReplica: Long,
) {
    init {
        require(replicas.isNotEmpty() && loadPerReplica > 0)
    }
}

/** 移動元のレプリカを移動先へ移す計画。 */
data class ReplicaMigration(
    val rangeId: RangeId,
    val fromNodeId: String,
    val toNodeId: String,
)
