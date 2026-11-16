package net.kigawa.fortis.raft.snapshot

/** オフセットと終端フラグを持つスナップショット転送の1片。 */
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
