package net.kigawa.fortis.raft.transport.codec

object RaftRpcMessageType {
    const val REQUEST_VOTE_REQUEST: Byte = 1
    const val REQUEST_VOTE_RESPONSE: Byte = 2
    const val APPEND_ENTRIES_REQUEST: Byte = 3
    const val APPEND_ENTRIES_RESPONSE: Byte = 4
}
