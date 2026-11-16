package net.kigawa.fortis.raft.snapshot

/** スナップショットを転送上限に合わせて分割する。 */
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
            ((data.size.toLong() + maxChunkBytes - 1) / maxChunkBytes).toInt(),
        )
        var offset = 0
        while (offset < data.size) {
            val end = offset + minOf(maxChunkBytes, data.size - offset)
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
