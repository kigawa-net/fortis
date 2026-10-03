package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult

class InMemoryRaftRpcChannel(
    private val codec: RaftRpcCodec = RaftRpcCodec(),
) : RaftRpcChannel {
    private val handlers = mutableMapOf<String, RaftRpcHandler>()

    fun register(nodeId: String, handler: RaftRpcHandler) {
        require(nodeId !in handlers) { "Node already registered: $nodeId" }
        handlers[nodeId] = handler
    }

    override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
        val handler = handlers[peerId] ?: throw RaftPeerUnavailableException(peerId)
        val message = when (val result = codec.decode(frame)) {
            is RaftRpcDecodeResult.Success -> {
                if (result.bytesRead != frame.size) {
                    throw RaftTransportException(
                        "Unexpected trailing bytes in Raft RPC request",
                    )
                }
                result.message
            }

            RaftRpcDecodeResult.Incomplete -> throw RaftTransportException(
                "Incomplete Raft RPC request",
            )

            is RaftRpcDecodeResult.Corrupted -> throw RaftTransportException(
                "Corrupted Raft RPC request: ${result.reason}",
            )
        }
        return codec.encode(handler.handle(message))
    }
}
