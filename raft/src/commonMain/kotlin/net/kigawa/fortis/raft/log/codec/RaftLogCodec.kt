package net.kigawa.fortis.raft.log.codec

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.RaftLogEntryPayload

class RaftLogCodec(
    val version: Byte = 1,
) {
    val headerSize: Int = 30

    fun encode(entry: RaftLogEntry): ByteArray {
        val commandType: Byte
        val key: ByteArray
        val value: ByteArray?
        when (val payload = entry.payload) {
            is RaftLogEntryPayload.Command -> when (val command = payload.command) {
                is RaftCommand.Put -> {
                    commandType = PUT
                    key = command.key
                    value = command.value
                }

                is RaftCommand.Delete -> {
                    commandType = DELETE
                    key = command.key
                    value = null
                }
            }

            RaftLogEntryPayload.NoOp -> {
                commandType = NO_OP
                key = byteArrayOf()
                value = null
            }
        }
        val valueLength = value?.size ?: -1
        val payloadSize = key.size.toLong() + maxOf(valueLength, 0).toLong()
        require(payloadSize <= Int.MAX_VALUE - headerSize) {
            "Raft log entry is too large"
        }
        val buffer = ByteArray(headerSize + payloadSize.toInt())
        MAGIC.copyInto(buffer)
        buffer[4] = version
        writeLong(buffer, 5, entry.index)
        writeLong(buffer, 13, entry.term)
        buffer[21] = commandType
        writeInt(buffer, 22, key.size)
        writeInt(buffer, 26, valueLength)
        key.copyInto(buffer, headerSize)
        value?.copyInto(buffer, headerSize + key.size)
        return buffer
    }

    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): RaftLogDecodeResult {
        if (offset < 0 || offset > data.size) {
            return RaftLogDecodeResult.Corrupted("Invalid offset: $offset")
        }
        if (data.size - offset < headerSize) {
            return RaftLogDecodeResult.Incomplete
        }
        for (index in MAGIC.indices) {
            if (data[offset + index] != MAGIC[index]) {
                return RaftLogDecodeResult.Corrupted("Invalid Raft log magic")
            }
        }
        val storedVersion = data[offset + 4]
        if (storedVersion != version) {
            return RaftLogDecodeResult.Corrupted(
                "Unsupported Raft log version: $storedVersion",
            )
        }

        val index = readLong(data, offset + 5)
        if (index <= 0) {
            return RaftLogDecodeResult.Corrupted(
                "Invalid Raft log index: $index",
            )
        }
        val term = readLong(data, offset + 13)
        if (term < 0) {
            return RaftLogDecodeResult.Corrupted(
                "Invalid Raft log term: $term",
            )
        }
        val commandType = data[offset + 21]
        if (commandType != PUT && commandType != DELETE && commandType != NO_OP) {
            return RaftLogDecodeResult.Corrupted(
                "Unknown Raft command type: $commandType",
            )
        }
        val keyLength = readInt(data, offset + 22)
        val valueLength = readInt(data, offset + 26)
        if (keyLength < 0) {
            return RaftLogDecodeResult.Corrupted("Invalid key length: $keyLength")
        }
        when (commandType) {
            PUT -> if (valueLength < 0) {
                return RaftLogDecodeResult.Corrupted(
                    "PUT entry must contain a value",
                )
            }

            DELETE -> if (valueLength != -1) {
                return RaftLogDecodeResult.Corrupted(
                    "DELETE entry must not contain a value",
                )
            }
        }

        if (commandType == NO_OP && (keyLength != 0 || valueLength != -1)) {
            return RaftLogDecodeResult.Corrupted("NO_OP entry must not contain key or value")
        }

        val payloadSize = keyLength.toLong() + maxOf(valueLength, 0).toLong()
        val recordSize = headerSize.toLong() + payloadSize
        if (recordSize > Int.MAX_VALUE) {
            return RaftLogDecodeResult.Corrupted("Raft log entry is too large")
        }
        if (data.size.toLong() - offset < recordSize) {
            return RaftLogDecodeResult.Incomplete
        }

        val keyStart = offset + headerSize
        val keyEnd = keyStart + keyLength
        val key = data.copyOfRange(keyStart, keyEnd)
        val payload = when (commandType) {
            PUT -> RaftLogEntryPayload.Command(
                RaftCommand.Put(key, data.copyOfRange(keyEnd, keyEnd + valueLength)),
            )

            DELETE -> RaftLogEntryPayload.Command(RaftCommand.Delete(key))
            else -> RaftLogEntryPayload.NoOp
        }
        return RaftLogDecodeResult.Success(
            RaftLogEntry(index, term, payload),
            recordSize.toInt(),
        )
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
            'L'.code.toByte(),
            'G'.code.toByte(),
        )
        const val PUT: Byte = 1
        const val DELETE: Byte = 2
        const val NO_OP: Byte = 3
    }
}
