package net.kigawa.fortis.raft.snapshot

/**
 * チャンク分割されたスナップショット受信側の再構成器。
 *
 * [InstallSnapshotChunks.chunked] で分割された [InstallSnapshotChunk] を
 * [offset] の連続性・重複・[metadata] の一貫性・[isLast] の終端性を検証
 * しながら受け付け、終端片の受信完了時に再構成済み data を返す。
 * 紛失・重複・順序不正・不一致は例外で拒否する。未完成の受信状態は
 * [discard] で破棄できる。完成後の受付と終端後の追加受付は拒否する。
 *
 * 単一メッセージの [InstallSnapshotRequest] 経路は残し、本器は追加の
 * 転送経路として使う。Transport/codec への配線と base-index 前方圧縮は
 * 将来課題とする。
 */
class SnapshotReassembler {
    private var term: Long? = null
    private var leaderId: String? = null
    private var metadata: SnapshotMetadata? = null
    private var nextOffset: Long = 0
    private val received = ArrayList<Byte>()
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
        for (byte in chunk.data) {
            received.add(byte)
        }
        nextOffset += chunk.data.size
        if (chunk.isLast) {
            complete = true
            return received.toByteArray()
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
