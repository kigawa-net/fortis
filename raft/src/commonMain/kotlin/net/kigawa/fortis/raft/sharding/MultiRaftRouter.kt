package net.kigawa.fortis.raft.sharding

import net.kigawa.fortis.raft.RaftCommand

/**
 * 単一レンジを担当するグループの識別子。
 *
 * ルーティング計算専用の値であり、ストレージや Raft ランタイムへの参照は
 * 持たない。書き込みは `RaftRuntime.propose`、読み取りは `linearizableRead`
 * 等の quorum 確認を経由すること（将来の #23 以降で接続予定）。
 * 本ルーターから直接ストレージ I/O を行ってはならない。
 */
data class GroupHandle(
    val rangeId: String,
)

/**
 * 静的なレンジ構成に基づくマルチ Raft グループルーター。
 *
 * ルーティング計算専用であり、ストレージ I/O を一切行わない。
 * version は Raft log index でありグループローカルである。
 * グループ間でグローバルな順序は仮定しない。
 * リバランス（#23）・トランザクション（#24）は範囲外とし、
 * 構成変更時は本ルーターを再構築する。
 */
class MultiRaftRouter(
    val table: RangeTable,
    groups: Map<String, GroupHandle>,
) {
    val groups: Map<String, GroupHandle> = groups.toMap()

    init {
        val tableIds = table.ranges.map { it.rangeId }.toSet()
        require(groups.keys == tableIds) {
            "groups must cover exactly the table ranges: table=$tableIds groups=${groups.keys}"
        }
        for ((id, handle) in groups) {
            require(handle.rangeId == id) {
                "GroupHandle.rangeId mismatch: key=$id handle=${handle.rangeId}"
            }
        }
    }

    /** [key] を担当するグループハンドルを返す。 */
    fun route(key: ByteArray): GroupHandle {
        val range = table.route(key)
        return checkNotNull(groups[range.rangeId]) {
            "no group for range ${range.rangeId}"
        }
    }

    /** [key] を担当する rangeId を返す。 */
    fun rangeIdOf(key: ByteArray): String = route(key).rangeId

    /** [RaftCommand] の対象キーから担当グループを返す。 */
    fun routeCommand(command: RaftCommand): GroupHandle = when (command) {
        is RaftCommand.Put -> route(command.key)
        is RaftCommand.Delete -> route(command.key)
    }
}
