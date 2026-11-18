package net.kigawa.fortis.raft.transport

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.transport.codec.GroupRaftRpcMessage
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class GroupRaftRpcCodecTest {
    private val codec = RaftRpcCodec()
    private val vote = RequestVoteRequest(3, "node-1", 1, 2)

    @Test
    fun groupFrameRoundTripsWithoutChangingLegacyFrames() {
        val message = RaftRpcMessage.RequestVote(vote)
        val grouped = GroupRaftRpcMessage("区間-A", message)
        val legacy = codec.encode(message)
        val frame = codec.encodeGroup(grouped)
        assertEquals(grouped, codec.decodeGroup(frame))
        assertEquals(RaftRpcDecodeResult.Success(message, legacy.size), codec.decode(legacy))
        assertIs<RaftRpcDecodeResult.Corrupted>(codec.decode(frame))
        assertFailsWith<RaftTransportException> { codec.decodeGroup(legacy) }
        assertFailsWith<RaftTransportException> { codec.decodeGroup(frame.copyOf(frame.size - 1)) }
        assertFailsWith<RaftTransportException> { codec.decodeGroup(frame + byteArrayOf(0)) }
        assertFailsWith<RaftTransportException> {
            val invalidUtf8 = frame.copyOf()
            invalidUtf8[codec.headerSize + Int.SIZE_BYTES] = 0xff.toByte()
            codec.decodeGroup(invalidUtf8)
        }
    }

    @Test
    fun groupedTransportRejectsResponsesFromAnotherGroup() = runTest {
        val channel = object : RaftRpcChannel {
            override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
                assertEquals(GroupRaftRpcMessage("left", RaftRpcMessage.RequestVote(vote)), codec.decodeGroup(frame))
                val result = RaftRpcMessage.RequestVoteResult(RequestVoteResponse(vote.term, true))
                return codec.encodeGroup(GroupRaftRpcMessage("right", result))
            }
        }
        assertFailsWith<RaftTransportException> { RpcRaftTransport.forGroup("left", channel).requestVote("node-2", vote) }
    }
}
