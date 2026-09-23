package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RpcRaftTransport(
    private val channel: RaftRpcChannel,
    private val codec: RaftRpcCodec = RaftRpcCodec(),
) : RaftTransport {
    override suspend fun requestVote(
        peerId: String,
        request: RequestVoteRequest,
    ): RequestVoteResponse = when (
        val response = request(peerId, RaftRpcMessage.RequestVote(request))
    ) {
        is RaftRpcMessage.RequestVoteResult -> response.response
        else -> throw unexpectedResponse(response)
    }

    override suspend fun appendEntries(
        peerId: String,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = when (
        val response = request(peerId, RaftRpcMessage.AppendEntries(request))
    ) {
        is RaftRpcMessage.AppendEntriesResult -> response.response
        else -> throw unexpectedResponse(response)
    }

    private suspend fun request(
        peerId: String,
        message: RaftRpcMessage,
    ): RaftRpcMessage {
        val response = channel.request(peerId, codec.encode(message))
        return decodeFrame(response, "response from $peerId")
    }

    private fun decodeFrame(
        frame: ByteArray,
        description: String,
    ): RaftRpcMessage = when (val result = codec.decode(frame)) {
        is RaftRpcDecodeResult.Success -> {
            if (result.bytesRead != frame.size) {
                throw RaftTransportException(
                    "Unexpected trailing bytes in Raft RPC $description",
                )
            }
            result.message
        }

        RaftRpcDecodeResult.Incomplete -> throw RaftTransportException(
            "Incomplete Raft RPC $description",
        )

        is RaftRpcDecodeResult.Corrupted -> throw RaftTransportException(
            "Corrupted Raft RPC $description: ${result.reason}",
        )
    }

    private fun unexpectedResponse(response: RaftRpcMessage) =
        RaftTransportException(
            "Unexpected RPC response: ${response::class.simpleName}",
        )
}
