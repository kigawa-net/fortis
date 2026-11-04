package net.kigawa.fortis.raft.snapshot

/**
 * チャンク分割されたスナップショット送信側の分割ヘルパー。
 *
 * [data] を [maxChunkBytes] 以下の [InstallSnapshotChunk] 列に分割する。
 * 単一メッセージの [InstallSnapshotRequest] 経路は残し、本ヘルパーは
 * 追加の転送経路として使う。Transport/codec への配線と base-index
 * 前方圧縮は将来課題とする。
 */
object InstallSnapshotChunks {
    /**
     * スナップショットデータをチャンク列に分割する。
     *
     * 空の [data] は終端のみの単一チャンクになる。各チャンクの [InstallSnapshotChunk.offset]
     * は当該片の開始位置を示し、末尾片のみ [InstallSnapshotChunk.isLast] が真になる。
     */
    fun chunked(
        term: Long,
        leaderId: String,
        metadata: SnapshotMetadata,
        data: ByteArray,
        maxChunkBytes: Int,
    ): List<InstallSnapshotChunk> {
        require(term >= 0) {
            "Raft term must not be negative"
        }
        require(maxChunkBytes > 0) {
            "チャンク上限は正でなければならない"
        }
        if (data.isEmpty()) {
            return listOf(
                InstallSnapshotChunk(
                    term = term,
                    leaderId = leaderId,
                    metadata = metadata,
                    offset = 0,
                    data = ByteArray(0),
                    isLast = true,
                ),
            )
        }
        val chunks = ArrayList<InstallSnapshotChunk>(
            (data.size + maxChunkBytes - 1) / maxChunkBytes,
        )
        var offset = 0
        while (offset < data.size) {
            val end = minOf(offset + maxChunkBytes, data.size)
            chunks.add(
                InstallSnapshotChunk(
                    term = term,
                    leaderId = leaderId,
                    metadata = metadata,
                    offset = offset.toLong(),
                    data = data.copyOfRange(offset, end),
                    isLast = end == data.size,
                ),
            )
            offset = end
        }
        return chunks
    }
}
