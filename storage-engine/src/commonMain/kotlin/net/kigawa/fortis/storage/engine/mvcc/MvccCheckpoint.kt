package net.kigawa.fortis.storage.engine.mvcc

/** 再起動用の MVCC 状態。pin はプロセスの寿命に従い復元しない。 */
class MvccCheckpoint internal constructor(
    history: List<MvccHistoryRecord>,
    val latestVersion: Long,
    val issuedTimestamp: Long,
    val compactedFloor: Long,
    uncommittedVersions: Collection<Long>,
) {
    private val records = history.map { MvccHistoryRecord(it.key, it.version, it.value) }
    private val uncommitted = uncommittedVersions.toList()
    val history: List<MvccHistoryRecord>
        get() = records.map { MvccHistoryRecord(it.key, it.version, it.value) }
    val uncommittedVersions: List<Long> get() = uncommitted.toList()
}
