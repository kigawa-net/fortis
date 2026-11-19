package net.kigawa.fortis.storage.engine.mvcc

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.BytewiseOrder
import net.kigawa.fortis.storage.engine.hasBytePrefix

// バージョン付き履歴エントリ。value=null は削除マーカー。
class VersionedEntry(
    val version: Long,
    val value: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is VersionedEntry) return false
        if (version != other.version) return false
        if (value == null || other.value == null) return value == null && other.value == null
        return value.contentEquals(other.value)
    }

    override fun hashCode(): Int {
        var result = version.hashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}

// メモリ常駐の MVCC 実装
class MemoryMvccStorageEngine : VersionedFortisStorageEngine {
    private val histories = mutableMapOf<ByteArrayKey, MutableList<VersionedEntry>>()
    private var latest = 0L
    private var issuedTimestamp = 0L
    private val intents = mutableMapOf<Long, MvccWriteIntent>()
    private val staleIntents = mutableSetOf<Long>()
    // ピン留め中の version と参照数
    private val pins = mutableMapOf<Long, Int>()
    // compact 済みの上限。retain はこの版以下を拒否する。
    private var compactedFloor = 0L
    private val mutex = Mutex()

    override suspend fun get(key: ByteArray): ByteArray? = mutex.withLock {
        newestAt(histories[ByteArrayKey(key)], Long.MAX_VALUE)?.copyOf()
    }

    // 未指定版の put は次版として記録する
    override suspend fun put(key: ByteArray, value: ByteArray) {
        mutex.withLock { addLocked(key.copyOf(), value.copyOf(), nextTimestampLocked()) }
    }

    // 未指定版の delete は次版として記録する
    override suspend fun delete(key: ByteArray): Boolean {
        return mutex.withLock { removeLocked(key, nextTimestampLocked()) }
    }

    override suspend fun putAt(key: ByteArray, value: ByteArray, version: Long) {
        require(version > 0) { "version must be positive" }
        mutex.withLock { addLocked(key.copyOf(), value.copyOf(), version) }
    }

    override suspend fun deleteAt(key: ByteArray, version: Long): Boolean {
        require(version > 0) { "version must be positive" }
        return mutex.withLock { removeLocked(key, version) }
    }

    override suspend fun getAt(key: ByteArray, readVersion: Long): ByteArray? = mutex.withLock {
        require(readVersion >= compactedFloor) { "version $readVersion was compacted below $compactedFloor" }
        newestAt(histories[ByteArrayKey(key)], readVersion)?.copyOf()
    }

    override suspend fun versionOf(key: ByteArray): Long = mutex.withLock {
        histories[ByteArrayKey(key)]?.maxOfOrNull { it.version } ?: 0L
    }

    override suspend fun latestVersion(): Long = mutex.withLock { latest }

    override suspend fun snapshot(): MvccSnapshot = mutex.withLock { MvccSnapshot(latest) }

    override suspend fun retain(version: Long): MvccSnapshot = mutex.withLock {
        require(version in 1..latest) {
            "cannot retain version $version (latest=$latest)"
        }
        require(version > compactedFloor) {
            "cannot retain version $version lost to compact($compactedFloor)"
        }
        pins[version] = (pins[version] ?: 0) + 1
        MvccSnapshot(version)
    }

    override suspend fun retainLatest(): MvccSnapshot = mutex.withLock {
        check(latest >= 1) { "nothing to retain (latest=$latest)" }
        check(latest > compactedFloor) {
            "latest version $latest lost to compact($compactedFloor)"
        }
        pins[latest] = (pins[latest] ?: 0) + 1
        MvccSnapshot(latest)
    }

    override suspend fun release(snapshot: MvccSnapshot) {
        mutex.withLock {
            val count = pins[snapshot.version]
            check(count != null && count > 0) {
                "snapshot version ${snapshot.version} is not retained"
            }
            if (count == 1) {
                pins.remove(snapshot.version)
            } else {
                pins[snapshot.version] = count - 1
            }
        }
    }

    override suspend fun compact(upToVersion: Long) = mutex.withLock {
        val floor = pins.keys.minOrNull()
        require(floor == null || upToVersion < floor) {
            "compact($upToVersion) would destroy retained snapshot at version $floor"
        }
        compactLocked(upToVersion)
    }

    /** Raft から指定された境界未満を圧縮する。対象の版が pin 中なら見送る。 */
    suspend fun compactBelowSafePoint(safePoint: Long) = mutex.withLock {
        if (safePoint <= 0) return@withLock
        val upToVersion = minOf(latest, safePoint - 1)
        if (upToVersion <= compactedFloor || pins.keys.any { it <= upToVersion }) return@withLock
        compactLocked(upToVersion)
    }

    private fun compactLocked(upToVersion: Long) {
        for ((_, history) in histories) {
            val newer = history.filter { it.version > upToVersion }
            val older = history.filter { it.version <= upToVersion }
            if (older.size <= 1) continue
            val keep = older.maxBy { it.version }
            history.clear()
            history.addAll(newer + keep)
            history.sortBy { it.version }
        }
        if (upToVersion > compactedFloor) compactedFloor = upToVersion
    }

    /** 単一エンジン内で単調な timestamp を発行する。 */
    suspend fun nextTimestamp(): Long = mutex.withLock { nextTimestampLocked() }

    private fun nextTimestampLocked(): Long {
        val previous = maxOf(issuedTimestamp, latest, compactedFloor)
        check(previous < Long.MAX_VALUE) { "timestamp exhausted" }
        issuedTimestamp = previous + 1
        return issuedTimestamp
    }

    /** 値と競合検証用の版を同じ排他区間で読む。 */
    suspend fun readVersioned(key: ByteArray): MvccRead = mutex.withLock {
        val entry = histories[ByteArrayKey(key)]?.lastOrNull()
        MvccRead(key, entry?.version ?: 0L, entry?.value)
    }

    /** 書き込み版を予約する。value=null は削除。reads は commit 時に版を検証する。 */
    suspend fun beginWrite(
        key: ByteArray,
        value: ByteArray?,
        reads: List<MvccRead> = emptyList(),
    ): MvccWriteIntent = mutex.withLock {
        val intent = MvccWriteIntent(nextTimestampLocked(), key, value, reads)
        intents[intent.version] = intent
        intent
    }

    /** 予約版を原子的に確定する。公開済み版より古い予約は確定できない。 */
    suspend fun commit(intent: MvccWriteIntent): MvccCommitResult = mutex.withLock {
        check(intents[intent.version] === intent) { "write intent is not active in this engine" }
        intents.remove(intent.version)
        if (intent.reads.any { read ->
                (histories[ByteArrayKey(read.key)]?.lastOrNull()?.version ?: 0L) != read.version
            }) {
            staleIntents.add(intent.version)
            return@withLock MvccCommitResult.READ_CONFLICT
        }
        if (intent.version <= maxOf(latest, compactedFloor)) {
            staleIntents.add(intent.version)
            return@withLock MvccCommitResult.SUPERSEDED
        }
        val value = intent.value
        if (value == null) removeLocked(intent.key, intent.version)
        else addLocked(intent.key, value, intent.version)
        MvccCommitResult.COMMITTED
    }

    /** 未確定予約を stale として破棄する。 */
    suspend fun abort(intent: MvccWriteIntent) = mutex.withLock {
        check(intents[intent.version] === intent) { "write intent is not active in this engine" }
        intents.remove(intent.version)
        staleIntents.add(intent.version)
        Unit
    }

    /** この境界より低い版を GC できる。pin がなければ発行済み最大版の次を返す。 */
    suspend fun gcSafePoint(): Long = mutex.withLock { gcSafePointLocked() }

    private fun gcSafePointLocked(): Long = pins.keys.minOrNull() ?: run {
        val maximum = maxOf(issuedTimestamp, latest, compactedFloor)
        if (maximum == Long.MAX_VALUE) maximum else maximum + 1
    }

    /** pin を保護して履歴と stale 予約を GC し、削除した予約数を返す。 */
    suspend fun collectGarbage(): Int = mutex.withLock {
        val safePoint = gcSafePointLocked()
        compactLocked(minOf(latest, safePoint - 1))
        val removed = staleIntents.filter { it < safePoint }
        staleIntents.removeAll(removed.toSet())
        removed.size
    }

    /** 復旧用に履歴・floor・発行済み版・未確定予約を原子的に取得する。 */
    suspend fun checkpoint(): MvccCheckpoint = mutex.withLock {
        MvccCheckpoint(
            histories.flatMap { (key, history) ->
                history.map { MvccHistoryRecord(key.bytes, it.version, it.value) }
            },
            latest, issuedTimestamp, compactedFloor, intents.keys + staleIntents,
        )
    }

    /** 空のエンジンに復元する。旧プロセスの pin は解放し、未確定予約は stale にする。 */
    suspend fun restoreCheckpoint(checkpoint: MvccCheckpoint): Unit = mutex.withLock {
        check(histories.isEmpty() && issuedTimestamp == 0L && compactedFloor == 0L) {
            "checkpoint restore target must be empty"
        }
        for (record in checkpoint.history) {
            val value = record.value
            if (value == null) removeLocked(record.key, record.version)
            else addLocked(record.key, value, record.version)
        }
        latest = checkpoint.latestVersion
        issuedTimestamp = checkpoint.issuedTimestamp
        compactedFloor = checkpoint.compactedFloor
        staleIntents.addAll(checkpoint.uncommittedVersions)
    }

    /** Raft スナップショットで確定履歴と境界を置き換え、ローカルの pin と予約を破棄する。 */
    suspend fun installCommittedSnapshot(
        history: List<MvccHistoryRecord>,
        floor: Long,
        issuedTimestamp: Long,
    ): Unit = mutex.withLock {
        require(floor >= 0 && issuedTimestamp >= 0 && history.all { it.version in 1..issuedTimestamp }) {
            "Invalid MVCC snapshot metadata"
        }
        histories.clear()
        latest = 0
        intents.clear()
        staleIntents.clear()
        pins.clear()
        for (record in history) {
            val value = record.value
            if (value == null) removeLocked(record.key, record.version)
            else addLocked(record.key.copyOf(), value.copyOf(), record.version)
        }
        this.issuedTimestamp = maxOf(this.issuedTimestamp, issuedTimestamp)
        compactedFloor = floor
    }

    override suspend fun scanPrefixAt(
        prefix: ByteArray,
        readVersion: Long,
        startAfter: ByteArray?,
        limit: Int,
    ): MvccScanResult = mutex.withLock {
        require(limit > 0) { "limit must be positive" }
        require(readVersion in 0..latest) {
            "cannot scan at version $readVersion (latest=$latest)"
        }
        if (readVersion < compactedFloor) return@withLock MvccScanResult.Compacted(compactedFloor)
        val keys = histories.keys
            .filter { it.bytes.hasBytePrefix(prefix) }
            .filter { startAfter == null || BytewiseOrder.compare(it.bytes, startAfter) > 0 }
            .sortedWith { a, b -> BytewiseOrder.compare(a.bytes, b.bytes) }
        val entries = mutableListOf<MvccScanEntry>()
        for (key in keys) {
            val visible = newestEntryAt(histories.getValue(key), readVersion) ?: continue
            val value = visible.value ?: continue
            if (entries.size == limit) return@withLock MvccScanResult.Page(entries, hasMore = true)
            entries.add(MvccScanEntry(key.bytes, visible.version, value))
        }
        MvccScanResult.Page(entries, hasMore = false)
    }

    override suspend fun exportHistory(): List<MvccHistoryRecord> = mutex.withLock {
        histories.flatMap { (key, history) ->
            history.map { MvccHistoryRecord(key.bytes, it.version, it.value) }
        }.sortedBy { it.version }
    }

    // 呼び出し側で mutex 保持済みであること
    private fun addLocked(key: ByteArray, value: ByteArray, version: Long) {
        val history = histories.getOrPut(ByteArrayKey(key)) { mutableListOf() }
        history.add(VersionedEntry(version, value))
        // Raft 適用は昇順だが順不同呼び出しに備えて整列する
        history.sortBy { it.version }
        if (version > latest) latest = version
        if (version > issuedTimestamp) issuedTimestamp = version
    }

    // 呼び出し側で mutex 保持済みであること
    private fun removeLocked(key: ByteArray, version: Long): Boolean {
        val history = histories.getOrPut(ByteArrayKey(key.copyOf())) { mutableListOf() }
        // version 直前で見えていた値があれば true
        val existed = newestAt(history, version - 1) != null
        history.add(VersionedEntry(version, null))
        history.sortBy { it.version }
        if (version > latest) latest = version
        if (version > issuedTimestamp) issuedTimestamp = version
        return existed
    }

    // readVersion 以下の最新エントリ（tombstone を含む）。履歴なしは null。
    private fun newestEntryAt(history: List<VersionedEntry>, readVersion: Long): VersionedEntry? {
        var result: VersionedEntry? = null
        for (entry in history) {
            if (entry.version > readVersion) break
            result = entry
        }
        return result
    }

    // readVersion 以下の最新値。tombstone/履歴なしは null。
    private fun newestAt(history: List<VersionedEntry>?, readVersion: Long): ByteArray? {
        if (history == null) return null
        var result: ByteArray? = null
        var found = false
        for (entry in history) {
            if (entry.version > readVersion) break
            result = entry.value
            found = true
        }
        return if (found) result else null
    }
}
