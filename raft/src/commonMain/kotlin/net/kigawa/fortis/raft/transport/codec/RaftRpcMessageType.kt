package net.kigawa.fortis.raft.transport.codec

object RaftRpcMessageType {
    const val REQUEST_VOTE_REQUEST: Byte = 1
    const val REQUEST_VOTE_RESPONSE: Byte = 2
    const val APPEND_ENTRIES_REQUEST: Byte = 3
    const val APPEND_ENTRIES_RESPONSE: Byte = 4

    /** 単一メッセージの InstallSnapshot 要求・応答。チャンク分割転送は将来課題。 */
    const val INSTALL_SNAPSHOT_REQUEST: Byte = 5
    const val INSTALL_SNAPSHOT_RESPONSE: Byte = 6
}
