package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead

/** 共通File I/Oでスナップショットを原子的に保存し、破損を検出する。 */
class FileRaftSnapshotStore(
    private val file: FortisFile,
) {
    private val mutex = Mutex()
    private var publicationFailed = false
    private var recovered = false

    private fun checkPublication() {
        check(!publicationFailed) { "Snapshot publication failed; reopen before continuing" }
    }

    /**
     * スナップショットを保存し、sync 後に読み戻して一致を検証する。
     *
     * 検証に失敗した場合は [RaftSnapshotCorruptionException] を投げる。
     */
    suspend fun save(
        metadata: SnapshotMetadata,
        data: ByteArray,
    ) {
        val encoded = encode(metadata, data)
        mutex.withLock {
            checkPublication()
            try {
                replaceSnapshotFile(file, encoded)
            } catch (failure: Throwable) {
                publicationFailed = true
                throw failure
            }
            recovered = true
            val reread = decode(readAllLocked() ?: throw RaftSnapshotCorruptionException("保存直後の読み戻しに失敗した"))
            check(reread.first == metadata && reread.second.contentEquals(data)) {
                "保存直後の読み戻し検証に失敗した"
            }
        }
    }

    /** 保存済みスナップショットを読み戻す。未保存の場合は null を返す。破損時は例外を投げる。 */
    suspend fun load(): Pair<SnapshotMetadata, ByteArray>? = mutex.withLock {
        checkPublication()
        val bytes = readAllLocked() ?: return@withLock null
        val decoded = decode(bytes)
        if (!recovered) {
            syncSnapshotFile(file)
            recovered = true
        }
        decoded
    }

    /** 最新スナップショットのメタデータを返す。未保存の場合は null を返す。破損時は例外を投げる。 */
    suspend fun latestMetadata(): SnapshotMetadata? = load()?.first

    private suspend fun readAllLocked(): ByteArray? {
        var result: ByteArray? = null
        try {
            file.openRead { input ->
                val size = input.size()
                if (size < 0 || size > Int.MAX_VALUE) {
                    throw RaftSnapshotCorruptionException("不正なスナップショットサイズ: $size")
                }
                val data = ByteArray(size.toInt())
                var offset = 0
                while (offset < data.size) {
                    val read = input.readAt(
                        offset = offset.toLong(),
                        buffer = data,
                        bufferOffset = offset,
                        length = data.size - offset,
                    )
                    if (read <= 0) {
                        throw RaftSnapshotCorruptionException("スナップショット読み戻し中に EOF に達した: offset=$offset")
                    }
                    offset += read
                }
                result = data
            }
        } catch (_: FsFileNotFoundException) {
            return null
        }
        return result
    }

    private fun encode(
        metadata: SnapshotMetadata,
        data: ByteArray,
    ): ByteArray {
        require(data.size.toLong() <= Int.MAX_VALUE - HEADER_SIZE - 4) {
            "スナップショットデータが大きすぎる"
        }
        val buffer = ByteArray(HEADER_SIZE + data.size + 4)
        MAGIC.copyInto(buffer)
        buffer[4] = VERSION
        writeLong(buffer, 5, metadata.lastIncludedIndex)
        writeLong(buffer, 13, metadata.lastIncludedTerm)
        writeInt(buffer, 21, data.size)
        data.copyInto(buffer, HEADER_SIZE)
        writeInt(buffer, buffer.size - 4, snapshotChecksum(buffer, buffer.size - 4))
        return buffer
    }

    private fun decode(bytes: ByteArray): Pair<SnapshotMetadata, ByteArray> {
        if (bytes.size < HEADER_SIZE) {
            throw RaftSnapshotCorruptionException("スナップショットが短すぎる: size=${bytes.size}")
        }
        for (index in MAGIC.indices) {
            if (bytes[index] != MAGIC[index]) {
                throw RaftSnapshotCorruptionException("不正なスナップショットマジック")
            }
        }
        if (bytes[4] != VERSION && bytes[4] != 1.toByte()) {
            throw RaftSnapshotCorruptionException("未対応のスナップショット版: ${bytes[4]}")
        }
        val lastIncludedIndex = readLong(bytes, 5)
        val lastIncludedTerm = readLong(bytes, 13)
        val dataLength = readInt(bytes, 21)
        if (dataLength < 0) {
            throw RaftSnapshotCorruptionException("不正なスナップショット長: $dataLength")
        }
        if (bytes.size.toLong() != HEADER_SIZE.toLong() + dataLength + (if (bytes[4] == VERSION) 4 else 0)) {
            throw RaftSnapshotCorruptionException(
                "スナップショットのサイズ不一致: size=${bytes.size}, dataLength=$dataLength",
            )
        }
        if (bytes[4] == VERSION && readInt(bytes, bytes.size - 4) != snapshotChecksum(bytes, bytes.size - 4)) {
            throw RaftSnapshotCorruptionException("スナップショットのチェックサム不一致")
        }
        val metadata = try {
            SnapshotMetadata(lastIncludedIndex, lastIncludedTerm)
        } catch (cause: IllegalArgumentException) {
            throw RaftSnapshotCorruptionException("不正なスナップショット境界: index=$lastIncludedIndex term=$lastIncludedTerm", cause)
        }
        return metadata to bytes.copyOfRange(HEADER_SIZE, HEADER_SIZE + dataLength)
    }

    private fun writeInt(
        buffer: ByteArray,
        offset: Int,
        value: Int,
    ) {
        buffer[offset] = (value ushr 24).toByte()
        buffer[offset + 1] = (value ushr 16).toByte()
        buffer[offset + 2] = (value ushr 8).toByte()
        buffer[offset + 3] = value.toByte()
    }

    private fun readInt(
        buffer: ByteArray,
        offset: Int,
    ): Int =
        ((buffer[offset].toInt() and 0xff) shl 24) or
            ((buffer[offset + 1].toInt() and 0xff) shl 16) or
            ((buffer[offset + 2].toInt() and 0xff) shl 8) or
            (buffer[offset + 3].toInt() and 0xff)

    private fun writeLong(
        buffer: ByteArray,
        offset: Int,
        value: Long,
    ) {
        for (index in 0 until Long.SIZE_BYTES) {
            buffer[offset + index] = (value ushr (56 - index * 8)).toByte()
        }
    }

    private fun readLong(
        buffer: ByteArray,
        offset: Int,
    ): Long {
        var result = 0L
        for (index in 0 until Long.SIZE_BYTES) {
            result = (result shl 8) or (buffer[offset + index].toLong() and 0xffL)
        }
        return result
    }

    companion object {
        private val MAGIC = byteArrayOf(
            'F'.code.toByte(),
            'R'.code.toByte(),
            'S'.code.toByte(),
            'N'.code.toByte(),
        )
        private const val VERSION: Byte = 2
        private const val HEADER_SIZE = 25

        /**
         * スナップショット格納を開く。
         *
         * 既存ファイルが破損している場合は [RaftSnapshotCorruptionException] を投げ、
         * 未保存の場合は空の格納を返す。再起動直後の読み戻し検証に使う。
         */
        suspend fun open(file: FortisFile): FileRaftSnapshotStore {
            val store = FileRaftSnapshotStore(file)
            store.load()
            return store
        }
    }
}
