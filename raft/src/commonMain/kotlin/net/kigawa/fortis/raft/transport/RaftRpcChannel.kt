package net.kigawa.fortis.raft.transport

interface RaftRpcChannel {
    suspend fun request(
        peerId: String,
        frame: ByteArray,
    ): ByteArray
}
