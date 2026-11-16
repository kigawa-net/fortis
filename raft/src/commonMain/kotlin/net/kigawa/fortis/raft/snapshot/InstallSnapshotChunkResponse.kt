package net.kigawa.fortis.raft.snapshot

/** チャンク受信結果と次の受信位置。終端成功後だけ送信側は複製位置を進める。 */
data class InstallSnapshotChunkResponse(val term: Long, val accepted: Boolean, val nextOffset: Long) {
    init {
        require(term >= 0 && nextOffset >= 0)
    }
}
