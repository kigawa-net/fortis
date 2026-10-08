package net.kigawa.fortis.raft.sharding

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

/**
 * 単一 Raft グループの最小ハンドル。グループ毎に独立した
 * [VersionedFortisStorageEngine] インスタンスを持つことを前提とする。
 */
data class GroupHandle(
    val rangeId: String,
    val storage: VersionedFortisStorageEngine,
)

/**
 * 静的なレンジ構成に基づくマルチ Raft グループルーター。
 *
 * version は Raft log index でありグループローカルである。
 * グループ間でグローバルな順序は仮定しない。各グループは独立した
 * log index 系列で自身の [VersionedFortisStorageEngine] に適用する。
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

    /** [RaftCommand] の対象キーから担当グループを返す。 */
    fun routeCommand(command: RaftCommand): GroupHandle = when (command) {
        is RaftCommand.Put -> route(command.key)
        is RaftCommand.Delete -> route(command.key)
    }

    /** [key] 担当グループのエンジンに書き込む。 */
    suspend fun put(key: ByteArray, value: ByteArray) {
        route(key).storage.put(key, value)
    }

    /** [key] 担当グループのエンジンから読む。 */
    suspend fun get(key: ByteArray): ByteArray? =
        route(key).storage.get(key)

    /** [key] 担当グループのエンジンから削除する。 */
    suspend fun delete(key: ByteArray): Boolean =
        route(key).storage.delete(key)

    /** [key] 担当グループのエンジンに version 付きで書き込む。 */
    suspend fun putAt(key: ByteArray, value: ByteArray, version: Long) {
        route(key).storage.putAt(key, value, version)
    }

    /** [key] 担当グループのエンジンから snapshot 読みする。 */
    suspend fun getAt(key: ByteArray, readVersion: Long): ByteArray? =
        route(key).storage.getAt(key, readVersion)

    /**
     * 指定グループに [RaftCommand] を log index [version] で適用する。
     * version はグループローカルであり他グループの順序とは無関係。
     */
    suspend fun applyAt(rangeId: String, command: RaftCommand, version: Long) {
        val handle = checkNotNull(groups[rangeId]) { "unknown rangeId: $rangeId" }
        command.executeAt(handle.storage, version)
    }
}
