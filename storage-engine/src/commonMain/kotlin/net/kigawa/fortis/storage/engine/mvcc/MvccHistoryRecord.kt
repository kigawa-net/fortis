package net.kigawa.fortis.storage.engine.mvcc

// 1 版分の履歴。value=null は削除マーカー（tombstone）。スナップショット用。
class MvccHistoryRecord(
    key: ByteArray,
    val version: Long,
    value: ByteArray?,
) {
    val key: ByteArray = key.copyOf()
    val value: ByteArray? = value?.copyOf()
}
