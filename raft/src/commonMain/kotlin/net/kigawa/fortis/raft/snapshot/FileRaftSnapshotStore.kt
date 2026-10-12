package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.io.fs.openWrite

/**
 * 受信・適用したスナップショット（メタデータ＋データ）のファイル永続化。
 *
 * 保存は truncate-then-write の順序で行い、書き込み後に sync してから
 * 読み戻して検証するため、クラッシュ時の半端な内容は次回読み戻しで
 * [RaftSnapshotCorruptionException] として検出される。黙って初期化しない。
 * ファイルが存在しない場合の [load] は null を返す。
 *
 * 保存形式は最小の外枠（マジック `FRSN`＋版＋長さ付き）とする。
 * チャンク分割転送と base-index 前方圧縮の対応は将来課題であり、
 * ここでは単一世代のスナップショット全体のみを扱う。
 */
class FileRaftSnapshotStore(
    private val file: FortisFile,
) {
    private val mutex = Mutex()

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
            file.openWrite(isCreate = true) { output ->
                output.truncate(0)
                var offset = 0
                while (offset < encoded.size) {
                    val written = output.writeAt(
                        offset = offset.toLong(),
                        data = encoded,
                        dataOffset = offset,
                        length = encoded.size - offset,
                    )
                    check(written > 0) { "Raft snapshot write made no progress" }
                    offset += written
                }
                output.sync()
            }
            val reread = decode(readAllLocked() ?: throw RaftSnapshotCorruptionException("保存直後の読み戻しに失敗した"))
            check(reread.first == metadata && reread.second.contentEquals(data)) {
                "保存直後の読み戻し検証に失敗した"
            }
        }
    }

    /** 保存済みスナップショットを読み戻す。未保存の場合は null を返す。破損時は例外を投げる。 */
    suspend fun load(): Pair<SnapshotMetadata, ByteArray>? = mutex.withLock {
        val bytes = readAllLocked() ?: return@withLock null
        decode(bytes)
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
        require(data.size.toLong() <= Int.MAX_VALUE - HEADER_SIZE) {
            "スナップショットデータが大きすぎる"
        }
        val buffer = ByteArray(HEADER_SIZE + data.size)
        MAGIC.copyInto(buffer)
        buffer[4] = VERSION
        writeLong(buffer, 5, metadata.lastIncludedIndex)
        writeLong(buffer, 13, metadata.lastIncludedTerm)
        writeInt(buffer, 21, data.size)
        data.copyInto(buffer, HEADER_SIZE)
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
        if (bytes[4] != VERSION) {
            throw RaftSnapshotCorruptionException("未対応のスナップショット版: ${bytes[4]}")
        }
        val lastIncludedIndex = readLong(bytes, 5)
        val lastIncludedTerm = readLong(bytes, 13)
        val dataLength = readInt(bytes, 21)
        if (dataLength < 0) {
            throw RaftSnapshotCorruptionException("不正なスナップショット長: $dataLength")
        }
        if (bytes.size.toLong() != HEADER_SIZE.toLong() + dataLength) {
            throw RaftSnapshotCorruptionException(
                "スナップショットのサイズ不一致: size=${bytes.size}, dataLength=$dataLength",
            )
        }
        val metadata = try {
            SnapshotMetadata(lastIncludedIndex, lastIncludedTerm)
        } catch (cause: IllegalArgumentException) {
            throw RaftSnapshotCorruptionException("不正なスナップショット境界: index=$lastIncludedIndex term=$lastIncludedTerm", cause)
        }
        return metadata to bytes.copyOfRange(HEADER_SIZE, bytes.size)
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
        private const val VERSION: Byte = 1
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
