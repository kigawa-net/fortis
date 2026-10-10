package net.kigawa.fortis.raft.meta

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.sharding.RangeTable

/**
 * メタ状態の版付きスナップショット。
 *
 * 将来的に専用 Raft グループで複製する想定だが、今回はインメモリ保持である。
 */
data class MetaSnapshot(
    /** メタ全体の版。構成更新時は構成版に追従し、配置のみの更新時は +1 される。 */
    val version: Long,
    val membership: ClusterMembership,
    val placement: RangePlacement,
)

/**
 * クラスタ構成情報（メンバー + レンジ配置）を保持する単一の権威。
 *
 * [Mutex] で排他し、[updateMembership]・[updatePlacement] のたびに版を進める。
 * 配置計画の算出までを担当し、実データ移動は行わない（#24 範囲）。
 */
class MetaStore(
    private val rangeTable: RangeTable,
    initialMembership: ClusterMembership,
    initialPlacement: RangePlacement,
) {
    private val mutex = Mutex()
    private var snapshot: MetaSnapshot

    init {
        initialPlacement.validate(rangeTable, initialMembership)
        snapshot = MetaSnapshot(initialMembership.version, initialMembership, initialPlacement)
    }

    /** 現在のスナップショットを返す。 */
    suspend fun snapshot(): MetaSnapshot = mutex.withLock { snapshot }

    /**
     * 構成を [newMembership] に置き換え、版を進める。
     *
     * [newMembership.version] は現在の構成版より大きいこと、
     * 既存配置の担当がすべて新構成に残ることを要求する。
     * メタ全体の版は単調増加し、配置のみ更新で進んだ版より
     * 後戻りしない（次版 = max(構成版, 現版 + 1)）。
     */
    suspend fun updateMembership(newMembership: ClusterMembership): MetaSnapshot = mutex.withLock {
        require(newMembership.version > snapshot.membership.version) {
            "stale membership: current=${snapshot.membership.version} new=${newMembership.version}"
        }
        newMembership.let { m ->
            // 配置の担当が新構成から消える場合は先に配置更新が必要なため拒否する。
            for ((rangeId, memberId) in snapshot.placement.assignment) {
                require(m.members.containsKey(memberId)) {
                    "placement refers to removed member: range=$rangeId member=$memberId"
                }
            }
        }
        val next = maxOf(newMembership.version, snapshot.version + 1)
        snapshot = MetaSnapshot(next, newMembership, snapshot.placement)
        snapshot
    }

    /**
     * 配置を [newPlacement] に置き換え、版を +1 する。
     *
     * 新配置は [rangeTable] と現在構成に対する整合性検証を通すこと。
     */
    suspend fun updatePlacement(newPlacement: RangePlacement): MetaSnapshot = mutex.withLock {
        newPlacement.validate(rangeTable, snapshot.membership)
        snapshot = snapshot.copy(version = snapshot.version + 1, placement = newPlacement)
        snapshot
    }
}
