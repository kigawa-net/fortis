package net.kigawa.fortis.raft.transport

open class RaftTransportException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)

class RaftPeerUnavailableException(
    val peerId: String,
    cause: Throwable? = null,
) : RaftTransportException(
    "Raft peer is unavailable: $peerId",
    cause,
)
