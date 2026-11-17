package net.kigawa.fortis.storage.engine.mvcc

/** 競合検証用の読み取り結果。削除済みキーにも版を保持する。 */
class MvccRead internal constructor(key: ByteArray, val version: Long, value: ByteArray?) {
    private val storedKey = key.copyOf()
    private val storedValue = value?.copyOf()
    val key: ByteArray get() = storedKey.copyOf()
    val value: ByteArray? get() = storedValue?.copyOf()
}

/** beginWrite で予約された版。生成元のエンジンで一度だけ確定または破棄する。 */
class MvccWriteIntent internal constructor(
    val version: Long,
    key: ByteArray,
    value: ByteArray?,
    reads: List<MvccRead>,
) {
    private val storedKey = key.copyOf()
    private val storedValue = value?.copyOf()
    internal val reads = reads.toList()
    val key: ByteArray get() = storedKey.copyOf()
    val value: ByteArray? get() = storedValue?.copyOf()
}

enum class MvccCommitResult {
    COMMITTED,
    READ_CONFLICT,
    SUPERSEDED,
}
