package net.kigawa.fortis.raft.transport

interface RaftRpcServer {
    suspend fun start()
    suspend fun stop()
}
