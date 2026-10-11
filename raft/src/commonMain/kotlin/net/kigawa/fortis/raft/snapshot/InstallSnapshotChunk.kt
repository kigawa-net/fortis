package net.kigawa.fortis.raft.snapshot

/**
 * チャンク分割転送の1片を表す。
 *
 * 大きなスナップショット [data] を固定上限で分割し、受信側で [offset]
 * の連続性と [isLast] の終端性を検証しながら再構成する。
 * 単一メッセージの [InstallSnapshotRequest] 経路は残し、本型は追加の
 * 転送経路として使う。Transport/codec への配線と base-index 前方圧縮は
 * 将来課題とする。
 */
class InstallSnapshotChunk(
    val term: Long,
    val leaderId: String,
    val metadata: SnapshotMetadata,
    val offset: Long,
    data: ByteArray,
    val isLast: Boolean,
) {
    val data: ByteArray = data.copyOf()

    init {
        require(term >= 0) {
            "Raft term must not be negative"
        }
        require(offset >= 0) {
            "スナップショットチャンクのオフセットは負であってはならない"
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as InstallSnapshotChunk

        if (term != other.term) return false
        if (leaderId != other.leaderId) return false
        if (metadata != other.metadata) return false
        if (offset != other.offset) return false
        if (isLast != other.isLast) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + leaderId.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + offset.hashCode()
        result = 31 * result + isLast.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }

    override fun toString(): String =
        "InstallSnapshotChunk(term=$term, leaderId=$leaderId, metadata=$metadata, " +
            "offset=$offset, dataSize=${data.size}, isLast=$isLast)"
}
