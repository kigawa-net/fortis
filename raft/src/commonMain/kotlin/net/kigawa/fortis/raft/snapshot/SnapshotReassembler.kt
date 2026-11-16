package net.kigawa.fortis.raft.snapshot

/** 任期・送信元・境界・オフセットを検証してチャンクを再構成する。 */
class SnapshotReassembler {
    private var term: Long? = null
    private var leaderId: String? = null
    private var metadata: SnapshotMetadata? = null
    private var nextOffset: Long = 0
    private val received = ArrayList<ByteArray>()
    private var complete = false

    /** 次に受け付けるべきオフセット。 */
    val expectedOffset: Long get() = nextOffset

    /** 終端片まで受信済みの場合に真。 */
    val isComplete: Boolean get() = complete

    /**
     * チャンクを1片受け付ける。
     *
     * 終端片の受信完了時は再構成済み data の複製を返し、それ以外は null を返す。
     */
    fun accept(chunk: InstallSnapshotChunk): ByteArray? {
        check(!complete) {
            "スナップショットの再構成は既に完了している"
        }
        if (chunk.data.isEmpty() && !chunk.isLast) {
            throw IllegalArgumentException("終端でない空チャンクは受け付けられない")
        }
        val firstTerm = term
        val firstLeaderId = leaderId
        val firstMetadata = metadata
        if (firstTerm == null || firstLeaderId == null || firstMetadata == null) {
            term = chunk.term
            leaderId = chunk.leaderId
            metadata = chunk.metadata
        } else {
            require(chunk.term == firstTerm) {
                "スナップショットチャンクのタームが不一致である"
            }
            require(chunk.leaderId == firstLeaderId) {
                "スナップショットチャンクのリーダーIDが不一致である"
            }
            require(chunk.metadata == firstMetadata) {
                "スナップショットチャンクのメタデータが不一致である"
            }
        }
        require(chunk.offset == nextOffset) {
            if (chunk.offset < nextOffset) {
                "スナップショットチャンクが重複または順序不正である"
            } else {
                "スナップショットチャンクが欠落している"
            }
        }
        require(nextOffset + chunk.data.size <= Int.MAX_VALUE) { "スナップショットが大きすぎる" }
        received.add(chunk.data.copyOf())
        nextOffset += chunk.data.size
        if (chunk.isLast) {
            complete = true
            val result = ByteArray(nextOffset.toInt())
            var offset = 0
            for (part in received) {
                part.copyInto(result, offset)
                offset += part.size
            }
            return result
        }
        return null
    }

    /** 未完成の受信状態を破棄し、再利用可能にする。 */
    fun discard() {
        term = null
        leaderId = null
        metadata = null
        nextOffset = 0
        received.clear()
        complete = false
    }
}
