package net.kigawa.fortis.raft.snapshot

/**
 * 単一メッセージの InstallSnapshot RPC 要求。
 *
 * [data] はステートマシンスナップショットの不透明バイト列であり、
 * Raft ログ層は内容を解釈せず [RaftSnapshotApplier] へ引き渡す。
 * チャンク分割転送は今回対象外とし、将来拡張として残す。
 */
class InstallSnapshotRequest(
    val term: Long,
    val leaderId: String,
    val metadata: SnapshotMetadata,
    data: ByteArray,
) {
    val data: ByteArray = data.copyOf()

    init {
        require(term >= 0) {
            "Raft term must not be negative"
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as InstallSnapshotRequest

        if (term != other.term) return false
        if (leaderId != other.leaderId) return false
        if (metadata != other.metadata) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + leaderId.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }

    override fun toString(): String =
        "InstallSnapshotRequest(term=$term, leaderId=$leaderId, metadata=$metadata, dataSize=${data.size})"
}
