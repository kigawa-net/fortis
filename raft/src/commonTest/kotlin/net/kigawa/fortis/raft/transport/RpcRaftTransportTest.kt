package net.kigawa.fortis.raft.transport

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RpcRaftTransportTest {
    private val codec = RaftRpcCodec()

    @Test
    fun requestVoteEncodesRequestAndDecodesResponse() = runTest {
        val request = RequestVoteRequest(3, "candidate", 4, 2)
        val response = RequestVoteResponse(3, true)
        val channel = RespondingChannel(
            codec.encode(RaftRpcMessage.RequestVoteResult(response)),
        )

        assertEquals(response, RpcRaftTransport(channel, codec).requestVote("peer", request))
        assertEquals(
            RaftRpcMessage.RequestVote(request),
            decode(channel.requestFrame),
        )
    }

    @Test
    fun appendEntriesRejectsUnexpectedResponseType() = runTest {
        val channel = RespondingChannel(
            codec.encode(
                RaftRpcMessage.RequestVoteResult(RequestVoteResponse(1, false)),
            ),
        )
        val request = AppendEntriesRequest(
            term = 1,
            leaderId = "leader",
            prevLogIndex = 0,
            prevLogTerm = 0,
            entries = emptyList(),
            leaderCommit = 0,
        )

        val error = assertFailsWith<RaftTransportException> {
            RpcRaftTransport(channel, codec).appendEntries("peer", request)
        }

        assertTrue(error.message.orEmpty().startsWith("Unexpected RPC response"))
    }

    @Test
    fun incompleteResponseBecomesTransportException() = runTest {
        val request = RequestVoteRequest(1, "candidate", 0, 0)

        val error = assertFailsWith<RaftTransportException> {
            RpcRaftTransport(RespondingChannel(byteArrayOf()), codec)
                .requestVote("peer", request)
        }

        assertEquals("Incomplete Raft RPC response from peer", error.message)
    }

    private fun decode(frame: ByteArray): RaftRpcMessage =
        (codec.decode(frame) as net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult.Success).message

    private class RespondingChannel(
        private val responseFrame: ByteArray,
    ) : RaftRpcChannel {
        lateinit var requestFrame: ByteArray

        override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
            requestFrame = frame
            return responseFrame
        }
    }
}
