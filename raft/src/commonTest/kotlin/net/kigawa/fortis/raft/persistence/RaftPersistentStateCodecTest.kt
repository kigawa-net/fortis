package net.kigawa.fortis.raft.persistence

import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateDecodeResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftPersistentStateCodecTest {
    private val codec = RaftPersistentStateCodec()

    @Test
    fun stateWithVoteRoundTripsAsUtf8() {
        assertRoundTrip(RaftPersistentState(3, "ノード-2"))
    }

    @Test
    fun stateWithoutVoteRoundTrips() {
        assertRoundTrip(RaftPersistentState(4, null))
    }

    @Test
    fun truncatedHeaderIsIncomplete() {
        val encoded = codec.encode(RaftPersistentState())

        assertIs<RaftPersistentStateDecodeResult.Incomplete>(
            codec.decode(encoded.copyOf(codec.headerSize - 1)),
        )
    }

    @Test
    fun truncatedVoteIsIncomplete() {
        val encoded = codec.encode(RaftPersistentState(3, "node-2"))

        assertIs<RaftPersistentStateDecodeResult.Incomplete>(
            codec.decode(encoded.copyOf(encoded.size - 1)),
        )
    }

    @Test
    fun invalidMagicIsCorrupted() {
        val encoded = codec.encode(RaftPersistentState()).also { it[0] = 0 }

        assertIs<RaftPersistentStateDecodeResult.Corrupted>(
            codec.decode(encoded),
        )
    }

    @Test
    fun invalidVersionIsCorrupted() {
        val encoded = codec.encode(RaftPersistentState()).also { it[4]++ }

        assertIs<RaftPersistentStateDecodeResult.Corrupted>(
            codec.decode(encoded),
        )
    }

    @Test
    fun trailingBytesAreCorrupted() {
        val encoded = codec.encode(RaftPersistentState()) + byteArrayOf(0)

        assertIs<RaftPersistentStateDecodeResult.Corrupted>(
            codec.decode(encoded),
        )
    }

    @Test
    fun invalidUtf8VoteIsCorrupted() {
        val encoded = codec.encode(RaftPersistentState(3, "a"))
            .also { it[codec.headerSize] = 0x80.toByte() }

        assertIs<RaftPersistentStateDecodeResult.Corrupted>(
            codec.decode(encoded),
        )
    }

    private fun assertRoundTrip(state: RaftPersistentState) {
        val result = assertIs<RaftPersistentStateDecodeResult.Success>(
            codec.decode(codec.encode(state)),
        )

        assertEquals(state, result.state)
    }
}
