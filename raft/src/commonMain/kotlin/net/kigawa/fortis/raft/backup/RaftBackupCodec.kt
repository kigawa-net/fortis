package net.kigawa.fortis.raft.backup

import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateDecodeResult
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata

/**
 * [RaftBackup] の符号化。
 *
 * 既存 codec の出力を長さ付きレコードとして束ねるだけとし、
 * 新規バイナリ形式の発明は外枠の額縁に留める。配置は以下:
 * `[magic FRBK][version:1][stateLen:4][state][metaFlag:1][meta?16][entryCount:4]([entryLen:4][entry])*`
 * メタデータ有りの場合の16バイトは `[lastIncludedIndex:8][lastIncludedTerm:8]` である。
 * import はマジック・版・末尾余剰を検証し、不正時は
 * [RaftBackupCorruptionException] で失敗する（silent reset しない）。
 */
class RaftBackupCodec(
    val version: Byte = 1,
    private val stateCodec: RaftPersistentStateCodec = RaftPersistentStateCodec(),
    private val logCodec: RaftLogCodec = RaftLogCodec(),
) {
    fun export(backup: RaftBackup): ByteArray {
        val stateBytes = stateCodec.encode(RaftPersistentState(backup.term, backup.votedFor))
        val entryBytes = backup.entries.map { logCodec.encode(it) }
        val size = headerSize + 4 + stateBytes.size + 1 +
            (if (backup.metadata == null) 0 else metadataSize) +
            4 + entryBytes.sumOf { 4 + it.size }
        val buffer = ByteArray(size)
        var offset = 0
        MAGIC.copyInto(buffer, offset)
        offset += MAGIC.size
        buffer[offset++] = version
        offset = writeInt(buffer, offset, stateBytes.size)
        stateBytes.copyInto(buffer, offset)
        offset += stateBytes.size
        val metadata = backup.metadata
        if (metadata == null) {
            buffer[offset++] = 0
        } else {
            buffer[offset++] = 1
            offset = writeLong(buffer, offset, metadata.lastIncludedIndex)
            offset = writeLong(buffer, offset, metadata.lastIncludedTerm)
        }
        offset = writeInt(buffer, offset, entryBytes.size)
        for (entry in entryBytes) {
            offset = writeInt(buffer, offset, entry.size)
            entry.copyInto(buffer, offset)
            offset += entry.size
        }
        check(offset == size) { "Raft backup encoding size mismatch" }
        return buffer
    }

    fun import(data: ByteArray): RaftBackup {
        val reader = Reader(data)
        try {
            reader.expectMagic()
            reader.expectVersion(version)
            val stateBytes = reader.readLengthPrefixed("state")
            val state = decodeState(stateBytes)
            val metadata = reader.readMetadata()
            val entryCount = reader.readInt("entry count")
            if (entryCount < 0) {
                throw RaftBackupCorruptionException("Invalid Raft backup entry count: $entryCount")
            }
            val entries = ArrayList<RaftLogEntry>(minOf(entryCount, maxEntries))
            repeat(entryCount) { position ->
                val entryBytes = reader.readLengthPrefixed("log entry #$position")
                entries.add(decodeEntry(entryBytes, position))
            }
            reader.expectExhausted()
            return RaftBackup(state.currentTerm, state.votedFor, entries, metadata)
        } catch (failure: RaftBackupCorruptionException) {
            throw failure
        } catch (failure: Exception) {
            throw RaftBackupCorruptionException(
                "Invalid Raft backup: ${failure.message}",
                failure,
            )
        }
    }

    private fun decodeState(bytes: ByteArray): RaftPersistentState =
        when (val result = stateCodec.decode(bytes)) {
            is RaftPersistentStateDecodeResult.Success -> result.state
            RaftPersistentStateDecodeResult.Incomplete ->
                throw RaftBackupCorruptionException("Incomplete Raft backup state")

            is RaftPersistentStateDecodeResult.Corrupted ->
                throw RaftBackupCorruptionException("Corrupted Raft backup state: ${result.reason}")
        }

    private fun decodeEntry(bytes: ByteArray, position: Int): RaftLogEntry =
        when (val result = logCodec.decode(bytes)) {
            is RaftLogDecodeResult.Success -> {
                if (result.bytesRead != bytes.size) {
                    throw RaftBackupCorruptionException(
                        "Trailing bytes in Raft backup log entry #$position",
                    )
                }
                if (result.entry.index != position + 1L) {
                    throw RaftBackupCorruptionException(
                        "Non-contiguous Raft backup log entry #$position: index ${result.entry.index}",
                    )
                }
                result.entry
            }

            RaftLogDecodeResult.Incomplete ->
                throw RaftBackupCorruptionException("Incomplete Raft backup log entry #$position")

            is RaftLogDecodeResult.Corrupted ->
                throw RaftBackupCorruptionException(
                    "Corrupted Raft backup log entry #$position: ${result.reason}",
                )
        }

    private class Reader(private val data: ByteArray) {
        var offset: Int = 0

        fun expectMagic() {
            if (data.size - offset < MAGIC.size + 1) {
                throw RaftBackupCorruptionException("Raft backup is too short")
            }
            for (index in MAGIC.indices) {
                if (data[offset + index] != MAGIC[index]) {
                    throw RaftBackupCorruptionException("Invalid Raft backup magic")
                }
            }
            offset += MAGIC.size
        }

        fun expectVersion(expected: Byte) {
            val stored = data[offset++]
            if (stored != expected) {
                throw RaftBackupCorruptionException("Unsupported Raft backup version: $stored")
            }
        }

        fun readInt(what: String): Int {
            if (data.size - offset < 4) {
                throw RaftBackupCorruptionException("Truncated Raft backup $what")
            }
            val value = ((data[offset].toInt() and 0xff) shl 24) or
                ((data[offset + 1].toInt() and 0xff) shl 16) or
                ((data[offset + 2].toInt() and 0xff) shl 8) or
                (data[offset + 3].toInt() and 0xff)
            offset += 4
            return value
        }

        fun readLengthPrefixed(what: String): ByteArray {
            val length = readInt("$what length")
            if (length < 0 || data.size - offset < length) {
                throw RaftBackupCorruptionException("Truncated Raft backup $what")
            }
            val bytes = data.copyOfRange(offset, offset + length)
            offset += length
            return bytes
        }

        fun readMetadata(): SnapshotMetadata? {
            if (data.size - offset < 1) {
                throw RaftBackupCorruptionException("Truncated Raft backup snapshot flag")
            }
            return when (val flag = data[offset++]) {
                0.toByte() -> null
                1.toByte() -> {
                    if (data.size - offset < metadataSize) {
                        throw RaftBackupCorruptionException("Truncated Raft backup snapshot metadata")
                    }
                    val lastIncludedIndex = readLong()
                    val lastIncludedTerm = readLong()
                    try {
                        SnapshotMetadata(lastIncludedIndex, lastIncludedTerm)
                    } catch (failure: IllegalArgumentException) {
                        throw RaftBackupCorruptionException(
                            "Invalid Raft backup snapshot metadata: ${failure.message}",
                            failure,
                        )
                    }
                }

                else -> throw RaftBackupCorruptionException(
                    "Invalid Raft backup snapshot flag: $flag",
                )
            }
        }

        fun expectExhausted() {
            if (offset != data.size) {
                throw RaftBackupCorruptionException("Trailing bytes after Raft backup")
            }
        }

        private fun readLong(): Long {
            var result = 0L
            repeat(Long.SIZE_BYTES) {
                result = (result shl 8) or (data[offset++].toLong() and 0xffL)
            }
            return result
        }
    }

    private fun writeInt(buffer: ByteArray, offset: Int, value: Int): Int {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
        return offset + 4
    }

    private fun writeLong(buffer: ByteArray, offset: Int, value: Long): Int {
        for (index in 0 until Long.SIZE_BYTES) {
            buffer[offset + index] = (value ushr (56 - index * 8)).toByte()
        }
        return offset + Long.SIZE_BYTES
    }

    companion object {
        private val MAGIC = byteArrayOf(
            'F'.code.toByte(),
            'R'.code.toByte(),
            'B'.code.toByte(),
            'K'.code.toByte(),
        )
        private const val headerSize = 5
        private const val metadataSize = 16
        private const val maxEntries = 1_000_000
    }
}
