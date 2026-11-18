package net.kigawa.fortis.raft.transport

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.sharding.RaftGroupManager
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult

class InMemoryRaftRpcChannel(
    private val codec: RaftRpcCodec = RaftRpcCodec(),
) : RaftRpcChannel {
    private val managerLock = Mutex()
    private val managers = mutableMapOf<String, RaftGroupManager>()
    private val handlers = mutableMapOf<String, RaftRpcHandler>()

    fun register(nodeId: String, handler: RaftRpcHandler) {
        require(nodeId !in handlers) { "Node already registered: $nodeId" }
        handlers[nodeId] = handler
    }

    /** ノードのグループ付き RPC の受信先を登録する。 */
    suspend fun registerGroupManager(manager: RaftGroupManager) = managerLock.withLock {
        require(manager.nodeId !in managers) { "Node already registered: ${manager.nodeId}" }
        managers[manager.nodeId] = manager
    }

    override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
        if (codec.isGroupFrame(frame)) {
            val manager = managerLock.withLock { managers[peerId] } ?: throw RaftPeerUnavailableException(peerId)
            return codec.encodeGroup(manager.handleRpc(codec.decodeGroup(frame)))
        }
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
