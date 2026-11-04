package net.kigawa.fortis.raft.snapshot

/** 単一メッセージの InstallSnapshot RPC 応答。 */
data class InstallSnapshotResponse(
    val term: Long,
) {
    init {
        require(term >= 0) {
            "Raft term must not be negative"
        }
    }
}
