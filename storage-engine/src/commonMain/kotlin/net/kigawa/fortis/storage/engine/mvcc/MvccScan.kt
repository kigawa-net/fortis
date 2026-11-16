package net.kigawa.fortis.storage.engine.mvcc

// スキャンで返す 1 件。value は読み取り版で可視な値（tombstone は含まない）。
class MvccScanEntry(
    key: ByteArray,
    val version: Long,
    value: ByteArray,
) {
    val key: ByteArray = key.copyOf()
    val value: ByteArray = value.copyOf()
}

sealed interface MvccScanResult {
    // hasMore: 同じ条件で startAfter 以降に続きの可視キーが存在する。
    data class Page(
        val entries: List<MvccScanEntry>,
        val hasMore: Boolean,
    ) : MvccScanResult

    // 読み取り版が compact で失われている（floor > readVersion）。部分結果は返さない。
    data class Compacted(
        val floor: Long,
    ) : MvccScanResult
}
