package net.kigawa.fortis.raft.transport

data class RaftPeerAddress(
    val host: String,
    val port: Int,
) {
    init {
        require(host.isNotBlank()) { "Raft peer host must not be blank" }
        require(port in 0..65535) { "Raft peer port is out of range: $port" }
    }
}
