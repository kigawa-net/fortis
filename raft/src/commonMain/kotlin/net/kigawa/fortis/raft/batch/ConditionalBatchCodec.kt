package net.kigawa.fortis.raft.batch

import net.kigawa.fortis.storage.engine.mvcc.MvccHistoryRecord

/**
 * [ConditionalBatchRequest] と受付記録のバイナリ表現。
 *
 * 正規形は一意である（同じ内容は常に同じバイト列になる）。デコード時は正規形と一致しないものを
 * 拒否するため、同じ要求に対して二つの異なる表現が存在しない。不正な入力は [IllegalArgumentException]。
 */
object ConditionalBatchCodec {
    private const val VERSION: Byte = 1
    private const val CONDITION_ABSENT: Byte = 1
    private const val CONDITION_REVISION_EQUALS: Byte = 2
    private const val MUTATION_PUT: Byte = 3
    private const val MUTATION_DELETE: Byte = 4

    /** 条件と変更の正規形（requestDigest）。 */
    internal fun encodeRequestBody(
        conditions: List<KvCondition>,
        mutations: List<KvMutation>,
    ): ByteArray {
        val writer = BinaryWriter()
        writer.byte(VERSION)
        writer.int(conditions.size)
        for (condition in conditions) {
            when (condition) {
                is KvCondition.Absent -> {
                    writer.byte(CONDITION_ABSENT)
                    writer.bytes(condition.key)
                }

                is KvCondition.RevisionEquals -> {
                    writer.byte(CONDITION_REVISION_EQUALS)
                    writer.bytes(condition.key)
                    writer.long(condition.revision)
                }
            }
        }
        writer.int(mutations.size)
        for (mutation in mutations) {
            when (mutation) {
                is KvMutation.Put -> {
                    writer.byte(MUTATION_PUT)
                    writer.bytes(mutation.key)
                    writer.bytes(mutation.value)
                }

                is KvMutation.Delete -> {
                    writer.byte(MUTATION_DELETE)
                    writer.bytes(mutation.key)
                }
            }
        }
        return writer.toByteArray()
    }

    /** principal・requestId・正規形を束ねた Raft log / RPC 用のバイト列。 */
    fun encode(request: ConditionalBatchRequest): ByteArray {
        val writer = BinaryWriter()
        writer.string(request.principal)
        writer.string(request.requestId)
        writer.bytes(request.requestDigest)
        return writer.toByteArray()
    }

    fun decode(data: ByteArray): ConditionalBatchRequest {
        val reader = BinaryReader(data)
        val principal = reader.string()
        val requestId = reader.string()
        val body = reader.bytes()
        reader.requireEnd()
        val (conditions, mutations) = decodeRequestBody(body)
        val request = ConditionalBatchRequest(principal, requestId, conditions, mutations)
        require(request.requestDigest.contentEquals(body)) { "Conditional batch body is not canonical" }
        return request
    }

    private fun decodeRequestBody(body: ByteArray): Pair<List<KvCondition>, List<KvMutation>> {
        val reader = BinaryReader(body)
        require(reader.byte() == VERSION) { "Unsupported conditional batch version" }
        val conditionCount = reader.int()
        require(conditionCount in 0..ConditionalBatchRequest.MAX_ITEMS) { "Invalid condition count" }
        val conditions = List(conditionCount) {
            when (val tag = reader.byte()) {
                CONDITION_ABSENT -> KvCondition.Absent(reader.bytes())
                CONDITION_REVISION_EQUALS -> KvCondition.RevisionEquals(reader.bytes(), reader.long())
                else -> throw IllegalArgumentException("Unknown condition tag: $tag")
            }
        }
        val mutationCount = reader.int()
        require(mutationCount in 0..ConditionalBatchRequest.MAX_ITEMS) { "Invalid mutation count" }
        val mutations = List(mutationCount) {
            when (val tag = reader.byte()) {
                MUTATION_PUT -> KvMutation.Put(reader.bytes(), reader.bytes())
                MUTATION_DELETE -> KvMutation.Delete(reader.bytes())
                else -> throw IllegalArgumentException("Unknown mutation tag: $tag")
            }
        }
        reader.requireEnd()
        return conditions to mutations
    }

    /** スナップショット本体（MVCC 履歴と受付記録）。 */
    internal fun encodeSnapshot(
        history: List<MvccHistoryRecord>,
        receipts: List<RequestReceipt>,
    ): ByteArray {
        val writer = BinaryWriter()
        writer.byte(VERSION)
        writer.int(history.size)
        for (record in history) {
            writer.bytes(record.key)
            writer.long(record.version)
            val value = record.value
            if (value == null) {
                writer.byte(0)
            } else {
                writer.byte(1)
                writer.bytes(value)
            }
        }
        writer.int(receipts.size)
        for (receipt in receipts) {
            writer.string(receipt.principal)
            writer.string(receipt.requestId)
            writer.bytes(receipt.requestDigest)
            when (val outcome = receipt.outcome) {
                is BatchOutcome.Applied -> {
                    writer.byte(OUTCOME_APPLIED)
                    writer.long(outcome.raftIndex)
                }

                is BatchOutcome.Rejected -> {
                    writer.byte(OUTCOME_REJECTED)
                    writer.int(outcome.failedCondition)
                    writer.long(outcome.observedRevision)
                }
            }
        }
        return writer.toByteArray()
    }

    internal fun decodeSnapshot(data: ByteArray): Pair<List<MvccHistoryRecord>, List<RequestReceipt>> {
        val reader = BinaryReader(data)
        require(reader.byte() == VERSION) { "Unsupported conditional batch snapshot version" }
        val historyCount = reader.int()
        require(historyCount >= 0) { "Invalid history count" }
        val history = List(historyCount) {
            val key = reader.bytes()
            val version = reader.long()
            val value = when (val present = reader.byte()) {
                0.toByte() -> null
                1.toByte() -> reader.bytes()
                else -> throw IllegalArgumentException("Invalid history presence flag: $present")
            }
            MvccHistoryRecord(key, version, value)
        }
        val receiptCount = reader.int()
        require(receiptCount >= 0) { "Invalid receipt count" }
        val receipts = List(receiptCount) {
            val principal = reader.string()
            val requestId = reader.string()
            val digest = reader.bytes()
            val outcome = when (val tag = reader.byte()) {
                OUTCOME_APPLIED -> BatchOutcome.Applied(reader.long())
                OUTCOME_REJECTED -> BatchOutcome.Rejected(reader.int(), reader.long())
                else -> throw IllegalArgumentException("Unknown outcome tag: $tag")
            }
            RequestReceipt(principal, requestId, digest, outcome)
        }
        reader.requireEnd()
        return history to receipts
    }

    private const val OUTCOME_APPLIED: Byte = 1
    private const val OUTCOME_REJECTED: Byte = 2
}

private class BinaryWriter {
    private var buffer = ByteArray(64)
    private var size = 0

    fun byte(value: Byte) {
        ensure(1)
        buffer[size++] = value
    }

    fun int(value: Int) {
        ensure(4)
        for (shift in intArrayOf(24, 16, 8, 0)) {
            buffer[size++] = (value ushr shift).toByte()
        }
    }

    fun long(value: Long) {
        ensure(8)
        for (shift in 56 downTo 0 step 8) {
            buffer[size++] = (value ushr shift).toByte()
        }
    }

    fun bytes(value: ByteArray) {
        int(value.size)
        ensure(value.size)
        value.copyInto(buffer, size)
        size += value.size
    }

    fun string(value: String) = bytes(value.encodeToByteArray())

    fun toByteArray(): ByteArray = buffer.copyOf(size)

    private fun ensure(extra: Int) {
        if (size + extra <= buffer.size) return
        var capacity = buffer.size * 2
        while (capacity < size + extra) capacity *= 2
        buffer = buffer.copyOf(capacity)
    }
}

private class BinaryReader(private val data: ByteArray) {
    private var position = 0

    fun byte(): Byte {
        require(position + 1 <= data.size) { "Truncated conditional batch data" }
        return data[position++]
    }

    fun int(): Int {
        require(position + 4 <= data.size) { "Truncated conditional batch data" }
        var result = 0
        repeat(4) { result = (result shl 8) or (data[position++].toInt() and 0xff) }
        return result
    }

    fun long(): Long {
        require(position + 8 <= data.size) { "Truncated conditional batch data" }
        var result = 0L
        repeat(8) { result = (result shl 8) or (data[position++].toLong() and 0xffL) }
        return result
    }

    fun bytes(): ByteArray {
        val length = int()
        require(length >= 0 && length <= data.size - position) { "Invalid field length: $length" }
        val result = data.copyOfRange(position, position + length)
        position += length
        return result
    }

    fun string(): String {
        val raw = bytes()
        return try {
            raw.decodeToString(throwOnInvalidSequence = true)
        } catch (cause: Exception) {
            throw IllegalArgumentException("Invalid UTF-8 in conditional batch data", cause)
        }
    }

    fun requireEnd() {
        require(position == data.size) { "Trailing bytes in conditional batch data" }
    }
}
