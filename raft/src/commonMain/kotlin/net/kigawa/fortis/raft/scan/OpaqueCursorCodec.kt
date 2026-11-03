package net.kigawa.fortis.raft.scan

// cursor 形式: version(1) | revision(8) | prefixLen(4) | prefix | lastKeyLen(4) | lastKey | checksum(8)
// checksum は FNV-1a 64 で、破損検出のみを目的とする。真正性（改ざん耐性・認可）は保証しない（#34 の範囲）。
internal class DecodedCursor(
    val revision: Long,
    val lastKey: ByteArray,
)

internal object OpaqueCursorCodec {
    private const val VERSION: Byte = 1
    private const val CHECKSUM_SIZE = 8
    private const val FNV_OFFSET = -3750763034362895579L // 0xcbf29ce484222325
    private const val FNV_PRIME = 0x100000001b3L

    fun encode(prefix: ByteArray, revision: Long, lastKey: ByteArray): OpaqueCursor {
        val body = Writer()
        body.byte(VERSION)
        body.long(revision)
        body.bytes(prefix)
        body.bytes(lastKey)
        val payload = body.toByteArray()
        return OpaqueCursor(payload + checksum(payload).toBytesBigEndian())
    }

    /** 形式・チェックサム・prefix 一致を検証する。不正なら [InvalidScanCursorException]。 */
    fun decode(cursor: OpaqueCursor, prefix: ByteArray): DecodedCursor {
        val bytes = cursor.toBytes()
        if (bytes.size < CHECKSUM_SIZE + 1 + 8 + 4 + 4) throw InvalidScanCursorException("cursor is too short")
        val payloadSize = bytes.size - CHECKSUM_SIZE
        val expected = readLong(bytes, payloadSize)
        if (checksum(bytes.copyOf(payloadSize)) != expected) throw InvalidScanCursorException("cursor checksum mismatch")

        val reader = Reader(bytes, payloadSize)
        if (reader.byte() != VERSION) throw InvalidScanCursorException("unsupported cursor version")
        val revision = reader.long()
        if (revision < 0) throw InvalidScanCursorException("cursor revision is negative")
        val cursorPrefix = reader.bytes()
        val lastKey = reader.bytes()
        if (!reader.finished()) throw InvalidScanCursorException("cursor has trailing bytes")
        if (!cursorPrefix.contentEquals(prefix)) throw InvalidScanCursorException("cursor belongs to a different prefix")
        if (!lastKey.hasPrefixOf(prefix)) throw InvalidScanCursorException("cursor key is outside the prefix")
        return DecodedCursor(revision, lastKey)
    }

    private fun ByteArray.hasPrefixOf(prefix: ByteArray): Boolean =
        size >= prefix.size && prefix.indices.all { this[it] == prefix[it] }

    private fun checksum(data: ByteArray): Long {
        var hash = FNV_OFFSET
        for (b in data) {
            hash = (hash xor (b.toLong() and 0xFF)) * FNV_PRIME
        }
        return hash
    }

    private fun readLong(bytes: ByteArray, offset: Int): Long {
        var result = 0L
        for (i in 0 until 8) {
            result = (result shl 8) or (bytes[offset + i].toLong() and 0xFF)
        }
        return result
    }

    private fun Long.toBytesBigEndian(): ByteArray = ByteArray(8) { i -> (this ushr (56 - 8 * i)).toByte() }

    private class Writer {
        private val out = mutableListOf<Byte>()

        fun byte(value: Byte) {
            out.add(value)
        }

        fun long(value: Long) {
            out.addAll(value.toBytesBigEndian().toList())
        }

        fun bytes(value: ByteArray) {
            val length = value.size
            for (shift in listOf(24, 16, 8, 0)) out.add((length ushr shift).toByte())
            out.addAll(value.toList())
        }

        fun toByteArray(): ByteArray = out.toByteArray()
    }

    private class Reader(
        private val bytes: ByteArray,
        private val end: Int,
    ) {
        private var position = 0

        fun byte(): Byte {
            if (position + 1 > end) throw InvalidScanCursorException("truncated cursor")
            return bytes[position++]
        }

        fun long(): Long {
            if (position + 8 > end) throw InvalidScanCursorException("truncated cursor")
            val value = readLong(bytes, position)
            position += 8
            return value
        }

        fun bytes(): ByteArray {
            if (position + 4 > end) throw InvalidScanCursorException("truncated cursor")
            var length = 0
            for (i in 0 until 4) {
                length = (length shl 8) or (bytes[position + i].toInt() and 0xFF)
            }
            position += 4
            if (length < 0 || length > end - position) throw InvalidScanCursorException("invalid cursor length")
            val value = bytes.copyOfRange(position, position + length)
            position += length
            return value
        }

        fun finished(): Boolean = position == end
    }
}

internal class InvalidScanCursorException(reason: String) : IllegalArgumentException(reason)
