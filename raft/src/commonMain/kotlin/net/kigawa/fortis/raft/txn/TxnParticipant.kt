package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

private fun ByteArray?.contentEqualsNullable(other: ByteArray?): Boolean {
    if (this == null || other == null) return this == null && other == null
    return contentEquals(other)
}

/**
 * 2PC の参加者（単一 Raft グループの窓口）。
 *
 * [prepare] で read-set と write-set の競合を検証し、問題なければ書き込みを
 * ステージングする。[commit] でステージング済みを確定版として適用し、
 * [abort] で破棄する。版の採番は `latestVersion + 1` からの連番で単調性を保つ。
 */
class TxnParticipant(
    val groupId: String,
    val engine: VersionedFortisStorageEngine,
) {
    private val mutex = Mutex()
    private val staged: MutableMap<TxnId, Map<ByteArrayKey, ByteArray?>> = mutableMapOf()

    /**
     * read-set・write-set の競合検証とステージング。
     *
     * 自グループ分の読取キー・書込キーについて
     * `getAt(key, latest) == getAt(key, snapshot)` を確認する。
     * 不一致があれば競合とみなし false を返す。
     */
    suspend fun prepare(record: TxnRecord): Boolean = mutex.withLock {
        val snapshot = record.snapshotOf(groupId)
        val reads = record.readSet[groupId].orEmpty()
        val writes = record.writeSet[groupId].orEmpty()
        val keys = mutableSetOf<ByteArrayKey>()
        keys.addAll(reads.keys)
        keys.addAll(writes.keys)
        for (key in keys) {
            val atSnapshot = engine.getAt(key.bytes.copyOf(), snapshot)
            val atLatest = engine.getAt(key.bytes.copyOf(), Long.MAX_VALUE)
            if (!atSnapshot.contentEqualsNullable(atLatest)) return false
        }
        staged[record.id] = writes.mapValues { (_, value) -> value?.copyOf() }
        true
    }

    /** ステージング済みを確定版として適用する。未ステージング時は無操作。 */
    suspend fun commit(record: TxnRecord): Unit = mutex.withLock {
        val writes = staged.remove(record.id) ?: return
        for ((key, value) in writes) {
            val version = engine.latestVersion() + 1
            if (value == null) {
                engine.deleteAt(key.bytes.copyOf(), version)
            } else {
                engine.putAt(key.bytes.copyOf(), value.copyOf(), version)
            }
        }
    }

    /** ステージング済みを破棄する。 */
    suspend fun abort(record: TxnRecord): Unit = mutex.withLock {
        staged.remove(record.id)
    }

    /** テスト用のステージング残量確認。 */
    suspend fun stagedCount(): Int = mutex.withLock { staged.size }
}
