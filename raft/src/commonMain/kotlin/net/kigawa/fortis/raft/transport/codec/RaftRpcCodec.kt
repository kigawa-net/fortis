package net.kigawa.fortis.raft.transport.codec

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

class RaftRpcCodec(
    val version: Byte = 1,
    val maxPayloadLength: Int = DEFAULT_MAX_PAYLOAD_LENGTH,
) {
    val headerSize: Int = HEADER_SIZE

    init {
        require(maxPayloadLength >= 0) {
            "Maximum RPC payload length must not be negative"
        }
        require(maxPayloadLength <= Int.MAX_VALUE - HEADER_SIZE) {
            "Maximum RPC payload length is too large"
        }
    }

    fun encode(message: RaftRpcMessage): ByteArray {
        val type: Byte
        val payload = when (message) {
            is RaftRpcMessage.RequestVote -> {
                type = RaftRpcMessageType.REQUEST_VOTE_REQUEST
                encodeRequestVote(message.request)
            }

            is RaftRpcMessage.RequestVoteResult -> {
                type = RaftRpcMessageType.REQUEST_VOTE_RESPONSE
                encodeRequestVoteResponse(message.response)
            }

            is RaftRpcMessage.AppendEntries -> {
                type = RaftRpcMessageType.APPEND_ENTRIES_REQUEST
                encodeAppendEntries(message.request)
            }

            is RaftRpcMessage.AppendEntriesResult -> {
                type = RaftRpcMessageType.APPEND_ENTRIES_RESPONSE
                encodeAppendEntriesResponse(message.response)
            }
        }
        require(payload.size <= maxPayloadLength) {
            "Raft RPC payload is too large: ${payload.size}"
        }

        val frame = ByteArray(headerSize + payload.size)
        MAGIC.copyInto(frame)
        frame[4] = version
        frame[5] = type
        writeInt(frame, 6, payload.size)
        payload.copyInto(frame, headerSize)
        return frame
    }

    fun decode(
        data: ByteArray,
        offset: Int = 0,
    ): RaftRpcDecodeResult {
        if (offset < 0 || offset > data.size) {
            return RaftRpcDecodeResult.Corrupted("Invalid offset: $offset")
        }
        if (data.size - offset < headerSize) {
            return RaftRpcDecodeResult.Incomplete
        }
        for (index in MAGIC.indices) {
            if (data[offset + index] != MAGIC[index]) {
                return RaftRpcDecodeResult.Corrupted("Invalid Raft RPC magic")
            }
        }
        val storedVersion = data[offset + 4]
        if (storedVersion != version) {
            return RaftRpcDecodeResult.Corrupted(
                "Unsupported Raft RPC version: $storedVersion",
            )
        }
        val type = data[offset + 5]
        if (type !in MESSAGE_TYPES) {
            return RaftRpcDecodeResult.Corrupted(
                "Unknown Raft RPC message type: $type",
            )
        }
        val payloadLength = readInt(data, offset + 6)
        if (payloadLength < 0) {
            return RaftRpcDecodeResult.Corrupted(
                "Invalid Raft RPC payload length: $payloadLength",
            )
        }
        if (payloadLength > maxPayloadLength) {
            return RaftRpcDecodeResult.Corrupted(
                "Raft RPC payload is too large: $payloadLength",
            )
        }
        val frameSize = headerSize.toLong() + payloadLength.toLong()
        if (data.size.toLong() - offset.toLong() < frameSize) {
            return RaftRpcDecodeResult.Incomplete
        }

        val payloadOffset = offset + headerSize
        val payloadEnd = payloadOffset + payloadLength
        val message = decodePayload(type, data, payloadOffset, payloadEnd)
        return when (message) {
            is PayloadResult.Success -> RaftRpcDecodeResult.Success(
                message.message,
                frameSize.toInt(),
            )

            is PayloadResult.Corrupted -> RaftRpcDecodeResult.Corrupted(message.reason)
        }
    }

    private fun encodeRequestVote(request: RequestVoteRequest): ByteArray {
        requireTerm(request.term)
        require(request.lastLogIndex >= 0) { "Last log index must not be negative" }
        requireTerm(request.lastLogTerm)
        val candidateId = request.candidateId.encodeToByteArray()
        require(candidateId.size <= Int.MAX_VALUE - 28) {
            "Candidate ID is too large"
        }
        val payload = ByteArray(requirePayloadSize(28 + candidateId.size))
        writeLong(payload, 0, request.term)
        writeInt(payload, 8, candidateId.size)
        candidateId.copyInto(payload, 12)
        writeLong(payload, 12 + candidateId.size, request.lastLogIndex)
        writeLong(payload, 20 + candidateId.size, request.lastLogTerm)
        return payload
    }

    private fun encodeRequestVoteResponse(response: RequestVoteResponse): ByteArray {
        requireTerm(response.term)
        return ByteArray(requirePayloadSize(9)).also { payload ->
            writeLong(payload, 0, response.term)
            writeBoolean(payload, 8, response.voteGranted)
        }
    }

    private fun encodeAppendEntries(request: AppendEntriesRequest): ByteArray {
        requireTerm(request.term)
        require(request.prevLogIndex >= 0) { "Previous log index must not be negative" }
        requireTerm(request.prevLogTerm)
        require(request.leaderCommit >= 0) { "Leader commit index must not be negative" }
        val leaderId = request.leaderId.encodeToByteArray()
        val entrySizes = request.entries.map(::entryEncodedSize)
        val payloadSize = 40L + leaderId.size + entrySizes.sumOf { it.toLong() }
        require(payloadSize <= Int.MAX_VALUE) { "AppendEntries payload is too large" }
        val payload = ByteArray(requirePayloadSize(payloadSize.toInt()))
        var cursor = 0
        writeLong(payload, cursor, request.term)
        cursor += 8
        writeInt(payload, cursor, leaderId.size)
        cursor += 4
        leaderId.copyInto(payload, cursor)
        cursor += leaderId.size
        writeLong(payload, cursor, request.prevLogIndex)
        cursor += 8
        writeLong(payload, cursor, request.prevLogTerm)
        cursor += 8
        writeLong(payload, cursor, request.leaderCommit)
        cursor += 8
        writeInt(payload, cursor, request.entries.size)
        cursor += 4
        for ((index, entry) in request.entries.withIndex()) {
            encodeEntry(entry, payload, cursor)
            cursor += entrySizes[index]
        }
        return payload
    }

    private fun encodeAppendEntriesResponse(response: AppendEntriesResponse): ByteArray {
        requireTerm(response.term)
        return ByteArray(requirePayloadSize(9)).also { payload ->
            writeLong(payload, 0, response.term)
            writeBoolean(payload, 8, response.success)
        }
    }

    private fun entryEncodedSize(entry: RaftLogEntry): Int {
        val valueLength = when (val command = entry.command) {
            is RaftCommand.Put -> command.value.size
            is RaftCommand.Delete -> 0
        }
        val keyLength = when (val command = entry.command) {
            is RaftCommand.Put -> command.key.size
            is RaftCommand.Delete -> command.key.size
        }
        val entrySize = 25L + keyLength + valueLength
        require(entrySize <= Int.MAX_VALUE) { "Raft RPC log entry is too large" }
        return entrySize.toInt()
    }

    private fun encodeEntry(
        entry: RaftLogEntry,
        buffer: ByteArray,
        offset: Int,
    ) {
        val commandType: Byte
        val key: ByteArray
        val value: ByteArray?
        when (val command = entry.command) {
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
        val valueLength = value?.size ?: -1
        writeLong(buffer, offset, entry.index)
        writeLong(buffer, offset + 8, entry.term)
        buffer[offset + 16] = commandType
        writeInt(buffer, offset + 17, key.size)
        writeInt(buffer, offset + 21, valueLength)
        key.copyInto(buffer, offset + 25)
        value?.copyInto(buffer, offset + 25 + key.size)
    }

    private fun decodePayload(
        type: Byte,
        data: ByteArray,
        start: Int,
        end: Int,
    ): PayloadResult = when (type) {
        RaftRpcMessageType.REQUEST_VOTE_REQUEST -> decodeRequestVote(data, start, end)
        RaftRpcMessageType.REQUEST_VOTE_RESPONSE -> decodeRequestVoteResponse(data, start, end)
        RaftRpcMessageType.APPEND_ENTRIES_REQUEST -> decodeAppendEntries(data, start, end)
        else -> decodeAppendEntriesResponse(data, start, end)
    }

    private fun decodeRequestVote(data: ByteArray, start: Int, end: Int): PayloadResult {
        if (end - start < 28) return corrupted("Truncated RequestVote payload")
        val term = readLong(data, start)
        val candidateLength = readInt(data, start + 8)
        if (term < 0) return corrupted("Invalid RequestVote term: $term")
        if (candidateLength < 0) return corrupted("Invalid candidate ID length: $candidateLength")
        val expectedSize = 28L + candidateLength
        if (expectedSize != end.toLong() - start) {
            return corrupted("Invalid RequestVote payload length")
        }
        val candidateEnd = start + 12 + candidateLength
        val candidateId = decodeString(data, start + 12, candidateEnd)
            ?: return corrupted("Candidate ID is not valid UTF-8")
        val lastLogIndex = readLong(data, candidateEnd)
        val lastLogTerm = readLong(data, candidateEnd + 8)
        if (lastLogIndex < 0) return corrupted("Invalid last log index: $lastLogIndex")
        if (lastLogTerm < 0) return corrupted("Invalid last log term: $lastLogTerm")
        return PayloadResult.Success(
            RaftRpcMessage.RequestVote(
                RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm),
            ),
        )
    }

    private fun decodeRequestVoteResponse(
        data: ByteArray,
        start: Int,
        end: Int,
    ): PayloadResult {
        if (end - start != 9) return corrupted("Invalid RequestVote response length")
        val term = readLong(data, start)
        if (term < 0) return corrupted("Invalid RequestVote response term: $term")
        val granted = readBoolean(data[start + 8])
            ?: return corrupted("Invalid voteGranted value")
        return PayloadResult.Success(
            RaftRpcMessage.RequestVoteResult(RequestVoteResponse(term, granted)),
        )
    }

    private fun decodeAppendEntries(data: ByteArray, start: Int, end: Int): PayloadResult {
        if (end - start < 40) return corrupted("Truncated AppendEntries payload")
        var cursor = start
        val term = readLong(data, cursor)
        cursor += 8
        val leaderLength = readInt(data, cursor)
        cursor += 4
        if (term < 0) return corrupted("Invalid AppendEntries term: $term")
        if (leaderLength < 0) return corrupted("Invalid leader ID length: $leaderLength")
        if (leaderLength.toLong() > end.toLong() - cursor - 28L) {
            return corrupted("Invalid AppendEntries payload length")
        }
        val leaderEnd = cursor + leaderLength
        val leaderId = decodeString(data, cursor, leaderEnd)
            ?: return corrupted("Leader ID is not valid UTF-8")
        cursor = leaderEnd
        val prevLogIndex = readLong(data, cursor)
        cursor += 8
        val prevLogTerm = readLong(data, cursor)
        cursor += 8
        val leaderCommit = readLong(data, cursor)
        cursor += 8
        val entryCount = readInt(data, cursor)
        cursor += 4
        if (prevLogIndex < 0) return corrupted("Invalid previous log index: $prevLogIndex")
        if (prevLogTerm < 0) return corrupted("Invalid previous log term: $prevLogTerm")
        if (leaderCommit < 0) return corrupted("Invalid leader commit index: $leaderCommit")
        if (entryCount < 0) return corrupted("Invalid entry count: $entryCount")
        if (entryCount > (end - cursor) / MIN_ENTRY_SIZE) {
            return corrupted("Entry count exceeds AppendEntries payload")
        }
        val entries = ArrayList<RaftLogEntry>(entryCount)
        repeat(entryCount) {
            val decoded = decodeEntry(data, cursor, end)
            when (decoded) {
                is EntryResult.Success -> {
                    entries.add(decoded.entry)
                    cursor += decoded.bytesRead
                }

                is EntryResult.Corrupted -> return corrupted(decoded.reason)
            }
        }
        if (cursor != end) return corrupted("Trailing bytes in AppendEntries payload")
        return PayloadResult.Success(
            RaftRpcMessage.AppendEntries(
                AppendEntriesRequest(
                    term,
                    leaderId,
                    prevLogIndex,
                    prevLogTerm,
                    entries,
                    leaderCommit,
                ),
            ),
        )
    }

    private fun decodeAppendEntriesResponse(
        data: ByteArray,
        start: Int,
        end: Int,
    ): PayloadResult {
        if (end - start != 9) return corrupted("Invalid AppendEntries response length")
        val term = readLong(data, start)
        if (term < 0) return corrupted("Invalid AppendEntries response term: $term")
        val success = readBoolean(data[start + 8])
            ?: return corrupted("Invalid success value")
        return PayloadResult.Success(
            RaftRpcMessage.AppendEntriesResult(AppendEntriesResponse(term, success)),
        )
    }

    private fun decodeEntry(data: ByteArray, start: Int, end: Int): EntryResult {
        if (end - start < MIN_ENTRY_SIZE) return EntryResult.Corrupted("Truncated log entry")
        val index = readLong(data, start)
        val term = readLong(data, start + 8)
        val commandType = data[start + 16]
        val keyLength = readInt(data, start + 17)
        val valueLength = readInt(data, start + 21)
        if (index <= 0) return EntryResult.Corrupted("Invalid log index: $index")
        if (term < 0) return EntryResult.Corrupted("Invalid log term: $term")
        if (commandType != PUT && commandType != DELETE) {
            return EntryResult.Corrupted("Unknown Raft command type: $commandType")
        }
        if (keyLength < 0) return EntryResult.Corrupted("Invalid key length: $keyLength")
        when (commandType) {
            PUT -> if (valueLength < 0) {
                return EntryResult.Corrupted("PUT command must contain a value")
            }

            DELETE -> if (valueLength != -1) {
                return EntryResult.Corrupted("DELETE command must not contain a value")
            }
        }
        val entrySize = MIN_ENTRY_SIZE.toLong() + keyLength + maxOf(valueLength, 0)
        if (entrySize > end.toLong() - start) {
            return EntryResult.Corrupted("Truncated log entry payload")
        }
        val keyStart = start + MIN_ENTRY_SIZE
        val keyEnd = keyStart + keyLength
        val key = data.copyOfRange(keyStart, keyEnd)
        val command = if (commandType == PUT) {
            RaftCommand.Put(key, data.copyOfRange(keyEnd, keyEnd + valueLength))
        } else {
            RaftCommand.Delete(key)
        }
        return EntryResult.Success(
            RaftLogEntry(index, term, command),
            entrySize.toInt(),
        )
    }

    private fun decodeString(
        data: ByteArray,
        start: Int,
        end: Int,
    ): String? = try {
        data.decodeToString(start, end, throwOnInvalidSequence = true)
    } catch (_: Exception) {
        null
    }

    private fun requireTerm(term: Long) {
        require(term >= 0) { "Raft term must not be negative" }
    }

    private fun requirePayloadSize(size: Int): Int {
        require(size <= maxPayloadLength) {
            "Raft RPC payload is too large: $size"
        }
        return size
    }

    private fun writeBoolean(buffer: ByteArray, offset: Int, value: Boolean) {
        buffer[offset] = if (value) 1 else 0
    }

    private fun readBoolean(value: Byte): Boolean? = when (value) {
        0.toByte() -> false
        1.toByte() -> true
        else -> null
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
            buffer[offset + index] = (value ushr (56 - index * 8)).toByte()
        }
    }

    private fun readLong(buffer: ByteArray, offset: Int): Long {
        var result = 0L
        for (index in 0 until Long.SIZE_BYTES) {
            result = (result shl 8) or (buffer[offset + index].toLong() and 0xffL)
        }
        return result
    }

    private fun corrupted(reason: String) = PayloadResult.Corrupted(reason)

    private sealed interface PayloadResult {
        data class Success(val message: RaftRpcMessage) : PayloadResult
        data class Corrupted(val reason: String) : PayloadResult
    }

    private sealed interface EntryResult {
        data class Success(
            val entry: RaftLogEntry,
            val bytesRead: Int,
        ) : EntryResult

        data class Corrupted(val reason: String) : EntryResult
    }

    companion object {
        const val DEFAULT_MAX_PAYLOAD_LENGTH: Int = 16 * 1024 * 1024
        private const val HEADER_SIZE = 10
        private const val MIN_ENTRY_SIZE = 25
        private const val PUT: Byte = 1
        private const val DELETE: Byte = 2
        private val MAGIC = byteArrayOf(
            'F'.code.toByte(),
            'R'.code.toByte(),
            'P'.code.toByte(),
            'C'.code.toByte(),
        )
        private val MESSAGE_TYPES = setOf(
            RaftRpcMessageType.REQUEST_VOTE_REQUEST,
            RaftRpcMessageType.REQUEST_VOTE_RESPONSE,
            RaftRpcMessageType.APPEND_ENTRIES_REQUEST,
            RaftRpcMessageType.APPEND_ENTRIES_RESPONSE,
        )
    }
}
