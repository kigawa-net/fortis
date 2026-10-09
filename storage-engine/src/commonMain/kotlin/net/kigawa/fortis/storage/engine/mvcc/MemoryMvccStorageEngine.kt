package net.kigawa.fortis.storage.engine.mvcc

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.ByteArrayKey

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
    // ピン留め中の version と参照数
    private val pins = mutableMapOf<Long, Int>()
    private val mutex = Mutex()

    override suspend fun get(key: ByteArray): ByteArray? = mutex.withLock {
        newestAt(histories[ByteArrayKey(key)], Long.MAX_VALUE)?.copyOf()
    }

    // 未指定版の put は次版として記録する
    override suspend fun put(key: ByteArray, value: ByteArray) {
        mutex.withLock { addLocked(key.copyOf(), value.copyOf(), latest + 1) }
    }

    // 未指定版の delete は次版として記録する
    override suspend fun delete(key: ByteArray): Boolean {
        return mutex.withLock { removeLocked(key, latest + 1) }
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
        newestAt(histories[ByteArrayKey(key)], readVersion)?.copyOf()
    }

    override suspend fun latestVersion(): Long = mutex.withLock { latest }

    override suspend fun snapshot(): MvccSnapshot = mutex.withLock { MvccSnapshot(latest) }

    override suspend fun retain(version: Long): MvccSnapshot = mutex.withLock {
        require(version in 1..latest) {
            "cannot retain version $version (latest=$latest)"
        }
        pins[version] = (pins[version] ?: 0) + 1
        MvccSnapshot(version)
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
        for ((_, history) in histories) {
            val newer = history.filter { it.version > upToVersion }
            val older = history.filter { it.version <= upToVersion }
            if (older.size <= 1) continue
            val keep = older.maxBy { it.version }
            history.clear()
            history.addAll(newer + keep)
            history.sortBy { it.version }
        }
    }

    // 呼び出し側で mutex 保持済みであること
    private fun addLocked(key: ByteArray, value: ByteArray, version: Long) {
        val history = histories.getOrPut(ByteArrayKey(key)) { mutableListOf() }
        history.add(VersionedEntry(version, value))
        // Raft 適用は昇順だが順不同呼び出しに備えて整列する
        history.sortBy { it.version }
        if (version > latest) latest = version
    }

    // 呼び出し側で mutex 保持済みであること
    private fun removeLocked(key: ByteArray, version: Long): Boolean {
        val history = histories.getOrPut(ByteArrayKey(key.copyOf())) { mutableListOf() }
        // version 直前で見えていた値があれば true
        val existed = newestAt(history, version - 1) != null
        history.add(VersionedEntry(version, null))
        history.sortBy { it.version }
        if (version > latest) latest = version
        return existed
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
