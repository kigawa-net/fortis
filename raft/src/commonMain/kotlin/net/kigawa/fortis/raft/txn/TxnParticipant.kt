package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

/**
 * 2PC の参加者（単一 Raft グループの窓口）。
 *
 * [prepare] で read-set と write-set の競合を検証し、問題なければ書き込みを
 * ステージングする。[commit] でステージング済みを確定版として適用し、
 * [abort] で破棄する。版の採番は `latestVersion + 1` からの連番で単調性を保つ。
 *
 * 競合検出は版ベースで行う。読取キー・書込キーについて、自スナップショット
 * より新しい版が存在すれば競合とみなす（値比較では ABA を検出できないため）。
 * 書込キーには write-intent を保持し、commit/abort まで他トランザクションの
 * prepare を拒否する。外部からの直接エンジン書き込みも版が進むため検出できる。
 */
class TxnParticipant(
    val groupId: String,
    val engine: VersionedFortisStorageEngine,
) {
    private val mutex = Mutex()
    private val staged: MutableMap<TxnId, Map<ByteArrayKey, ByteArray?>> = mutableMapOf()
    // 書込予約: key -> 保持中のトランザクション。commit/abort まで保持する。
    private val intents: MutableMap<ByteArrayKey, TxnId> = mutableMapOf()

    /**
     * read-set・write-set の競合検証とステージング。
     *
     * 自グループ分の読取キー・書込キーについて、エンジン上の最新版が
     * スナップショット以下であることを確認する。書込キーが他トランザクションに
     * 予約済みの場合も競合とみなし false を返す。
     */
    suspend fun prepare(record: TxnRecord): Boolean = mutex.withLock {
        val snapshot = record.snapshotOf(groupId)
        val reads = record.readSet[groupId].orEmpty()
        val writes = record.writeSet[groupId].orEmpty()
        val keys = mutableSetOf<ByteArrayKey>()
        keys.addAll(reads.keys)
        keys.addAll(writes.keys)
        for (key in keys) {
            if (engine.versionOf(key.bytes.copyOf()) > snapshot) return false
        }
        for (key in writes.keys) {
            val holder = intents[key]
            if (holder != null && holder != record.id) return false
        }
        for (key in writes.keys) {
            intents[key] = record.id
        }
        staged[record.id] = writes.mapValues { (_, value) -> value?.copyOf() }
        true
    }

    /** ステージング済みを確定版として適用する。未ステージング時は無操作。 */
    suspend fun commit(record: TxnRecord): Unit = mutex.withLock {
        try {
            val writes = staged.remove(record.id) ?: return
            for ((key, value) in writes) {
                val version = engine.latestVersion() + 1
                if (value == null) {
                    engine.deleteAt(key.bytes.copyOf(), version)
                } else {
                    engine.putAt(key.bytes.copyOf(), value.copyOf(), version)
                }
            }
        } finally {
            releaseIntentsLocked(record.id)
        }
    }

    /** ステージング済みを破棄する。 */
    suspend fun abort(record: TxnRecord): Unit = mutex.withLock {
        staged.remove(record.id)
        releaseIntentsLocked(record.id)
    }

    /** テスト用のステージング残量確認。 */
    suspend fun stagedCount(): Int = mutex.withLock { staged.size }

    // 呼び出し側で mutex 保持済みであること
    private fun releaseIntentsLocked(id: TxnId) {
        val owned = intents.filterValues { it == id }.keys.toList()
        for (key in owned) intents.remove(key)
    }
}
