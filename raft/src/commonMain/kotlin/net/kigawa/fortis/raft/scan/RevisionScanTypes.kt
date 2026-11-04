package net.kigawa.fortis.raft.scan

/**
 * revision 固定の prefix scan の要求。
 *
 * - [readRevision]: null なら線形化可能読み取りで現在の適用済み revision を確定する。指定時は
 *   その revision（適用済み・compact 未満でないこと）で読む。[cursor] と併用する場合は cursor の revision と一致が必要。
 * - [cursor]: 前ページの [ScanResult.Page.nextCursor]。null なら先頭ページ。
 */
class ScanRequest(
    prefix: ByteArray,
    val readRevision: Long? = null,
    val cursor: OpaqueCursor? = null,
    val pageSize: Int,
) {
    val prefix: ByteArray = prefix.copyOf()

    init {
        require(this.prefix.size <= MAX_PREFIX_BYTES) { "Scan prefix exceeds $MAX_PREFIX_BYTES bytes" }
        require(pageSize in 1..MAX_PAGE_SIZE) { "pageSize must be in 1..$MAX_PAGE_SIZE" }
        require(readRevision == null || readRevision >= 0) { "readRevision must not be negative" }
    }

    companion object {
        const val MAX_PREFIX_BYTES = 1024
        const val MAX_PAGE_SIZE = 1000
    }
}

/** スキャン結果の 1 件。value は [ScanResult.Page.revision] 時点で可視な値。 */
class ScanEntry(
    key: ByteArray,
    val value: ByteArray,
    val revision: Long,
) {
    val key: ByteArray = key.copyOf()

    override fun equals(other: Any?): Boolean =
        other is ScanEntry && key.contentEquals(other.key) && value.contentEquals(other.value) && revision == other.revision

    override fun hashCode(): Int = 31 * (31 * key.contentHashCode() + value.contentHashCode()) + revision.hashCode()

    override fun toString(): String = "ScanEntry(revision=$revision, keySize=${key.size}, valueSize=${value.size})"
}

/**
 * 不透明な再開位置。revision・prefix・最後に返したキーを含む。
 * 中身は呼び出し側が解釈・改変してはならない。改変は [ScanResult.InvalidCursor] で拒否される。
 */
class OpaqueCursor(bytes: ByteArray) {
    private val data: ByteArray = bytes.copyOf()

    /** 符号化済みバイト列のコピー。 */
    fun toBytes(): ByteArray = data.copyOf()

    override fun equals(other: Any?): Boolean = other is OpaqueCursor && data.contentEquals(other.data)

    override fun hashCode(): Int = data.contentHashCode()

    override fun toString(): String = "OpaqueCursor(size=${data.size})"
}

sealed interface ScanResult {
    /**
     * 1 ページ。[nextCursor] が null のときのみ prefix の列挙が完了している。
     * 続きがあるのに null を返すことはない（部分結果を完全結果として見せない）。
     */
    data class Page(
        val entries: List<ScanEntry>,
        val revision: Long,
        val nextCursor: OpaqueCursor?,
    ) : ScanResult

    /** 固定 revision が compact で失われた。部分結果は返さず、新しい revision で最初からやり直す必要がある。 */
    data class RevisionCompacted(val floor: Long) : ScanResult

    /** 要求された revision がまだ適用されていない（現在の最大適用 revision は [latest]）。 */
    data class RevisionUnavailable(val revision: Long, val latest: Long) : ScanResult

    /** cursor が不正（形式不一致・チェックサム不一致・別 prefix・revision 不一致）。 */
    data class InvalidCursor(val reason: String) : ScanResult

    /** 線形化可能読み取りを確定できなかった（非 Leader・Leader 交代・停止・タイムアウト）。再試行可能。 */
    data class Unavailable(val reason: String) : ScanResult

    /** この状態機械では revision 固定スキャンを提供できない（非 MVCC 等）。 */
    data class Unsupported(val reason: String) : ScanResult
}
