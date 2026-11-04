package net.kigawa.fortis.raft.txn

import net.kigawa.fortis.storage.engine.ByteArrayKey

/**
 * 単一トランザクションの読み書き集合と状態。
 *
 * [snapshotVersions] はグループごとの読取 version（スナップショット分離の起点）。
 * version は Raft log index でありグループローカル。
 * [readSet] は観測値（null は不在）、[writeSet] は確定予定値（null は削除）を групповごとに保持する。
 */
class TxnRecord(
    val id: TxnId,
    val snapshotVersions: Map<String, Long>,
    var state: TxnState = TxnState.PREPARING,
) {
    val readSet: MutableMap<String, MutableMap<ByteArrayKey, ByteArray?>> = mutableMapOf()
    val writeSet: MutableMap<String, MutableMap<ByteArrayKey, ByteArray?>> = mutableMapOf()

    /** [groupId] の読取スナップショット version。未指定時は 0（空読み）。 */
    fun snapshotOf(groupId: String): Long = snapshotVersions[groupId] ?: 0L

    /** 書き込み対象のグループ集合。 */
    fun writeGroups(): Set<String> = writeSet.keys.toSet()
}
