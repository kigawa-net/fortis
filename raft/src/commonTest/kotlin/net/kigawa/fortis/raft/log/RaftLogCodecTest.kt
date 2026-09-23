package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftLogCodecTest {
    private val codec = RaftLogCodec()

    @Test
    fun putEntryRoundTrips() {
        assertRoundTrip(
            RaftLogEntry(
                index = 1,
                term = 2,
                command = RaftCommand.Put(
                    byteArrayOf(1, 2),
                    byteArrayOf(10, 20),
                ),
            ),
        )
    }

    @Test
    fun deleteEntryRoundTrips() {
        assertRoundTrip(
            RaftLogEntry(
                index = 3,
                term = 4,
                command = RaftCommand.Delete(byteArrayOf(1, 2)),
            ),
        )
    }

    @Test
    fun truncatedHeaderIsIncomplete() {
        val bytes = codec.encode(entry()).copyOf(codec.headerSize - 1)

        assertIs<RaftLogDecodeResult.Incomplete>(codec.decode(bytes))
    }

    @Test
    fun truncatedPayloadIsIncomplete() {
        val encoded = codec.encode(entry())

        assertIs<RaftLogDecodeResult.Incomplete>(
            codec.decode(encoded.copyOf(encoded.size - 1)),
        )
    }

    @Test
    fun invalidMagicIsCorrupted() {
        val encoded = codec.encode(entry()).also { it[0] = 0 }

        assertIs<RaftLogDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun invalidVersionIsCorrupted() {
        val encoded = codec.encode(entry()).also { it[4]++ }

        assertIs<RaftLogDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun invalidCommandTypeIsCorrupted() {
        val encoded = codec.encode(entry()).also { it[21] = Byte.MAX_VALUE }

        assertIs<RaftLogDecodeResult.Corrupted>(codec.decode(encoded))
    }

    private fun assertRoundTrip(entry: RaftLogEntry) {
        val encoded = codec.encode(entry)
        val result = assertIs<RaftLogDecodeResult.Success>(
            codec.decode(encoded),
        )

        assertEquals(entry, result.entry)
        assertEquals(encoded.size, result.bytesRead)
    }

    private fun entry() = RaftLogEntry(
        index = 1,
        term = 2,
        command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10)),
    )
}
