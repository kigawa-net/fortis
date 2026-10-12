package net.kigawa.fortis.raft.transport

fun interface RaftPeerResolver {
    fun resolve(peerId: String): RaftPeerAddress
}
