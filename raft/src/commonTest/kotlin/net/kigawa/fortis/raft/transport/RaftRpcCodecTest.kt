package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftRpcCodecTest {
    private val codec = RaftRpcCodec()

    @Test
    fun requestVoteRequestRoundTrips() {
        assertRoundTrip(
            RaftRpcMessage.RequestVote(
                RequestVoteRequest(
                    term = 3,
                    candidateId = "候補-1",
                    lastLogIndex = 10,
                    lastLogTerm = 2,
                ),
            ),
        )
    }

    @Test
    fun requestVoteResponseRoundTrips() {
        assertRoundTrip(
            RaftRpcMessage.RequestVoteResult(
                RequestVoteResponse(term = 3, voteGranted = true),
            ),
        )
    }

    @Test
    fun appendEntriesHeartbeatRoundTrips() {
        assertRoundTrip(
            RaftRpcMessage.AppendEntries(
                AppendEntriesRequest(
                    term = 4,
                    leaderId = "node-1",
                    prevLogIndex = 5,
                    prevLogTerm = 3,
                    entries = emptyList(),
                    leaderCommit = 4,
                ),
            ),
        )
    }

    @Test
    fun appendEntriesWithMultipleEntriesRoundTrips() {
        assertRoundTrip(
            RaftRpcMessage.AppendEntries(
                AppendEntriesRequest(
                    term = 4,
                    leaderId = "node-1",
                    prevLogIndex = 1,
                    prevLogTerm = 2,
                    entries = listOf(
                        RaftLogEntry(
                            index = 2,
                            term = 4,
                            command = RaftCommand.Put(
                                key = byteArrayOf(1, 2),
                                value = byteArrayOf(10, 20),
                            ),
                        ),
                        RaftLogEntry(
                            index = 3,
                            term = 4,
                            command = RaftCommand.Delete(byteArrayOf(3, 4)),
                        ),
                    ),
                    leaderCommit = 2,
                ),
            ),
        )
    }

    @Test
    fun appendEntriesResponseRoundTrips() {
        assertRoundTrip(
            RaftRpcMessage.AppendEntriesResult(
                AppendEntriesResponse(term = 4, success = false),
            ),
        )
    }

    @Test
    fun consecutiveFramesCanBeDecodedByOffset() {
        val first = RaftRpcMessage.RequestVoteResult(RequestVoteResponse(1, true))
        val second = RaftRpcMessage.AppendEntriesResult(AppendEntriesResponse(2, false))
        val firstFrame = codec.encode(first)
        val frames = firstFrame + codec.encode(second)

        val firstResult = assertIs<RaftRpcDecodeResult.Success>(codec.decode(frames))
        val secondResult = assertIs<RaftRpcDecodeResult.Success>(
            codec.decode(frames, firstResult.bytesRead),
        )

        assertEquals(first, firstResult.message)
        assertEquals(firstFrame.size, firstResult.bytesRead)
        assertEquals(second, secondResult.message)
    }

    @Test
    fun invalidMagicIsCorrupted() {
        val encoded = encodedMessage().also { it[0] = 0 }

        assertIs<RaftRpcDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun invalidVersionIsCorrupted() {
        val encoded = encodedMessage().also { it[4]++ }

        assertIs<RaftRpcDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun unknownMessageTypeIsCorrupted() {
        val encoded = encodedMessage().also { it[5] = Byte.MAX_VALUE }

        assertIs<RaftRpcDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun truncatedHeaderIsIncomplete() {
        val encoded = encodedMessage().copyOf(codec.headerSize - 1)

        assertIs<RaftRpcDecodeResult.Incomplete>(codec.decode(encoded))
    }

    @Test
    fun truncatedPayloadIsIncomplete() {
        val encoded = encodedMessage()

        assertIs<RaftRpcDecodeResult.Incomplete>(
            codec.decode(encoded.copyOf(encoded.size - 1)),
        )
    }

    @Test
    fun oversizedPayloadLengthIsCorruptedBeforeAllocation() {
        val limitedCodec = RaftRpcCodec(maxPayloadLength = 64)
        val encoded = limitedCodec.encode(
            RaftRpcMessage.RequestVoteResult(RequestVoteResponse(1, true)),
        )
        writeInt(encoded, 6, 65)

        assertIs<RaftRpcDecodeResult.Corrupted>(limitedCodec.decode(encoded))
    }

    private fun assertRoundTrip(message: RaftRpcMessage) {
        val encoded = codec.encode(message)
        val result = assertIs<RaftRpcDecodeResult.Success>(codec.decode(encoded))

        assertEquals(message, result.message)
        assertEquals(encoded.size, result.bytesRead)
    }

    private fun encodedMessage(): ByteArray = codec.encode(
        RaftRpcMessage.RequestVoteResult(RequestVoteResponse(1, true)),
    )

    private fun writeInt(buffer: ByteArray, offset: Int, value: Int) {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
    }
}
