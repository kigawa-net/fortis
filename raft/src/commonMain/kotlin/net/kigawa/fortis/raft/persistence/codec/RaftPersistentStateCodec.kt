package net.kigawa.fortis.raft.persistence.codec

import net.kigawa.fortis.raft.RaftPersistentState

class RaftPersistentStateCodec(
    val version: Byte = 2,
) {
    init { require(version == 1.toByte() || version == 2.toByte()) { "Unsupported state encoding version" } }

    val headerSize: Int = 17

    fun encode(state: RaftPersistentState): ByteArray {
        require(state.currentTerm >= 0) {
            "Raft term must not be negative"
        }
        val votedFor = state.votedFor?.encodeToByteArray()
        val votedForLength = votedFor?.size ?: -1
        val checksumSize = if (version == 2.toByte()) 4 else 0
        require(maxOf(votedForLength, 0) <= Int.MAX_VALUE - headerSize - checksumSize) { "State is too large" }
        val buffer = ByteArray(headerSize + maxOf(votedForLength, 0) + checksumSize)
        MAGIC.copyInto(buffer)
        buffer[4] = version
        writeLong(buffer, 5, state.currentTerm)
        writeInt(buffer, 13, votedForLength)
        votedFor?.copyInto(buffer, headerSize)
        if (checksumSize != 0) writeInt(buffer, buffer.size - 4, crc32(buffer, buffer.size - 4))
        return buffer
    }

    fun decode(data: ByteArray): RaftPersistentStateDecodeResult {
        if (data.size < headerSize) {
            return RaftPersistentStateDecodeResult.Incomplete
        }
        for (index in MAGIC.indices) {
            if (data[index] != MAGIC[index]) {
                return RaftPersistentStateDecodeResult.Corrupted(
                    "Invalid Raft persistent state magic",
                )
            }
        }
        val storedVersion = data[4]
        if (storedVersion != 1.toByte() && storedVersion != 2.toByte()) {
            return RaftPersistentStateDecodeResult.Corrupted(
                "Unsupported Raft persistent state version: $storedVersion",
            )
        }
        val term = readLong(data, 5)
        if (term < 0) {
            return RaftPersistentStateDecodeResult.Corrupted(
                "Invalid Raft term: $term",
            )
        }
        val votedForLength = readInt(data, 13)
        if (votedForLength < -1) {
            return RaftPersistentStateDecodeResult.Corrupted(
                "Invalid votedFor length: $votedForLength",
            )
        }
        val recordSize = headerSize.toLong() + maxOf(votedForLength, 0).toLong() +
            if (storedVersion == 2.toByte()) 4 else 0
        if (recordSize > Int.MAX_VALUE) {
            return RaftPersistentStateDecodeResult.Corrupted(
                "Raft persistent state is too large",
            )
        }
        if (data.size.toLong() < recordSize) {
            return RaftPersistentStateDecodeResult.Incomplete
        }
        if (data.size.toLong() > recordSize) {
            return RaftPersistentStateDecodeResult.Corrupted(
                "Trailing bytes after Raft persistent state",
            )
        }

        if (storedVersion == 2.toByte() && readInt(data, data.size - 4) != crc32(data, data.size - 4)) {
            return RaftPersistentStateDecodeResult.Corrupted("Raft persistent state checksum mismatch")
        }

        val votedFor = if (votedForLength == -1) {
            null
        } else {
            try {
                data.decodeToString(
                    startIndex = headerSize,
                    endIndex = headerSize + votedForLength,
                    throwOnInvalidSequence = true,
                )
            } catch (_: Exception) {
                return RaftPersistentStateDecodeResult.Corrupted(
                    "votedFor is not valid UTF-8",
                )
            }
        }
        return RaftPersistentStateDecodeResult.Success(
            RaftPersistentState(term, votedFor),
        )
    }

    private fun crc32(data: ByteArray, length: Int): Int {
        var crc = -1
        for (i in 0 until length) {
            crc = crc xor (data[i].toInt() and 0xff)
            repeat(8) { crc = (crc ushr 1) xor if ((crc and 1) != 0) 0xedb88320.toInt() else 0 }
        }
        return crc.inv()
    }

    private fun writeInt(buffer: ByteArray, offset: Int, value: Int) {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
    }

    private fun readInt(buffer: ByteArray, offset: Int): Int =
        ((buffer[offset].toInt() and 0xff) shl 24) or
            ((buffer[offset + 1].toInt() and 0xff) shl 16) or
            ((buffer[offset + 2].toInt() and 0xff) shl 8) or
            (buffer[offset + 3].toInt() and 0xff)

    private fun writeLong(buffer: ByteArray, offset: Int, value: Long) {
        for (index in 0 until Long.SIZE_BYTES) {
            buffer[offset + index] =
                (value ushr (56 - index * 8)).toByte()
        }
    }

    private fun readLong(buffer: ByteArray, offset: Int): Long {
        var result = 0L
        for (index in 0 until Long.SIZE_BYTES) {
            result = (result shl 8) or
                (buffer[offset + index].toLong() and 0xffL)
        }
        return result
    }

    private companion object {
        val MAGIC = byteArrayOf(
            'F'.code.toByte(),
            'R'.code.toByte(),
            'S'.code.toByte(),
            'T'.code.toByte(),
        )
    }
}
