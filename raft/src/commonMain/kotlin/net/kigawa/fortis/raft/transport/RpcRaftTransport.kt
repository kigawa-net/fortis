package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunk
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunkResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.transport.codec.GroupRaftRpcMessage
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RpcRaftTransport private constructor(
    private val channel: RaftRpcChannel,
    private val codec: RaftRpcCodec,
    private val groupId: String?,
) : RaftTransport {
    constructor(channel: RaftRpcChannel, codec: RaftRpcCodec = RaftRpcCodec()) : this(channel, codec, null)

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

    override suspend fun installSnapshot(
        peerId: String,
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse = when (
        val response = request(peerId, RaftRpcMessage.InstallSnapshot(request))
    ) {
        is RaftRpcMessage.InstallSnapshotResult -> response.response
        else -> throw unexpectedResponse(response)
    }

    override suspend fun installSnapshotChunk(peerId: String, chunk: InstallSnapshotChunk): InstallSnapshotChunkResponse =
        when (val response = request(peerId, RaftRpcMessage.SnapshotChunk(chunk))) {
            is RaftRpcMessage.SnapshotChunkResult -> response.response
            else -> throw unexpectedResponse(response)
        }

    private suspend fun request(
        peerId: String,
        message: RaftRpcMessage,
    ): RaftRpcMessage {
        if (groupId != null) {
            val response = codec.decodeGroup(channel.request(peerId, codec.encodeGroup(GroupRaftRpcMessage(groupId, message))))
            if (response.groupId != groupId) {
                throw RaftTransportException("Expected Raft group $groupId, received ${response.groupId}")
            }
            return response.message
        }
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

    companion object {
        /** 共有チャネル上で指定グループ宛ての Raft RPC を送受信する。 */
        fun forGroup(groupId: String, channel: RaftRpcChannel, codec: RaftRpcCodec = RaftRpcCodec()): RpcRaftTransport {
            require(groupId.isNotEmpty()) { "Raft group ID must not be empty" }
            return RpcRaftTransport(channel, codec, groupId)
        }
    }
}
