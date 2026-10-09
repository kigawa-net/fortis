package net.kigawa.fortis.raft.txn

import net.kigawa.fortis.storage.engine.ByteArrayKey

/**
 * 2PC のコーディネーター。
 *
 * スナップショット分離で読み、prepare（全参加者）→全 OK なら commit、
 * 1つでも NG なら abort する。単一グループも参加者1名の 2PC として扱う。
 * 記録はインメモリのみであり、Raft ログへの永続化は将来課題とする。
 */
class TxnCoordinator(
    participants: Map<String, TxnParticipant>,
    private val idGenerator: TxnIdGenerator = TxnIdGenerator(),
) {
    val participants: Map<String, TxnParticipant> = participants.toMap()
    private val records: MutableMap<TxnId, TxnRecord> = mutableMapOf()

    /** 記録済みトランザクションの一覧（テスト・回復用）。 */
    fun records(): Map<TxnId, TxnRecord> = records.toMap()

    /** 各参加者の最新版をスナップショットとして開始する。 */
    suspend fun begin(): Txn {
        val snapshots = participants.mapValues { (_, participant) ->
            participant.engine.latestVersion()
        }
        return begin(snapshots)
    }

    /** 指定スナップショットで開始する。 */
    fun begin(snapshotVersions: Map<String, Long>): Txn {
        val record = TxnRecord(idGenerator.nextId(), snapshotVersions.toMap())
        records[record.id] = record
        return Txn(record)
    }

    /**
     * 単一トランザクションの操作面。
     *
     * get はスナップショット読み（自 write-set を優先）。
     * commit は 2PC を実行し、成功時 true を返す。
     */
    inner class Txn internal constructor(val record: TxnRecord) {
        val id: TxnId get() = record.id

        /** スナップショットから読む。読取値は read-set に記録される。 */
        suspend fun get(groupId: String, key: ByteArray): ByteArray? {
            val participant = requireParticipant(groupId)
            val wrapped = ByteArrayKey(key.copyOf())
            val pending = record.writeSet[groupId]
            if (pending != null && pending.containsKey(wrapped)) {
                return pending.getValue(wrapped)?.copyOf()
            }
            val snapshot = record.snapshotOf(groupId)
            val observed = participant.engine.getAt(key.copyOf(), snapshot)?.copyOf()
            record.readSet.getOrPut(groupId) { mutableMapOf() }
                .putIfAbsent(wrapped, observed?.copyOf())
            return observed?.copyOf()
        }

        /** 書き込みを write-set に蓄える（即時反映しない）。 */
        fun put(groupId: String, key: ByteArray, value: ByteArray) {
            requireParticipant(groupId)
            record.writeSet.getOrPut(groupId) { mutableMapOf() }[ByteArrayKey(key.copyOf())] =
                value.copyOf()
        }

        /** 削除を write-set に蓄える（即時反映しない）。 */
        fun delete(groupId: String, key: ByteArray) {
            requireParticipant(groupId)
            record.writeSet.getOrPut(groupId) { mutableMapOf() }[ByteArrayKey(key.copyOf())] = null
        }

        /**
         * 2PC で確定する。読みのみは即 COMMITTED。
         *
         * @return commit 成功時 true、競合等で abort 時 false。
         */
        suspend fun commit(): Boolean {
            val groups = record.writeGroups().toList()
            if (groups.isEmpty()) {
                record.state = TxnState.COMMITTED
                return true
            }
            record.state = TxnState.PREPARING
            val prepared = mutableListOf<TxnParticipant>()
            for (groupId in groups) {
                val participant = requireParticipant(groupId)
                if (!participant.prepare(record)) {
                    for (done in prepared) done.abort(record)
                    record.state = TxnState.ABORTED
                    return false
                }
                prepared.add(participant)
            }
            record.state = TxnState.PREPARED
            for (participant in prepared) participant.commit(record)
            record.state = TxnState.COMMITTED
            return true
        }

        /** 明示的に破棄する。 */
        suspend fun abort() {
            for (groupId in record.writeGroups()) {
                participants[groupId]?.abort(record)
            }
            record.state = TxnState.ABORTED
        }

        private fun requireParticipant(groupId: String): TxnParticipant =
            checkNotNull(participants[groupId]) { "unknown group: $groupId" }
    }

    /**
     * PREPARING / PREPARED のまま残った記録を abort する（presumed-abort）。
     *
     * コーディネーター障害後に未確定の記録を解決する手順を表す。
     *
     * @return abort した記録数。
     */
    suspend fun recover(): Int {
        var count = 0
        for (record in records.values) {
            if (record.state == TxnState.PREPARING || record.state == TxnState.PREPARED) {
                for (groupId in record.writeGroups()) {
                    participants[groupId]?.abort(record)
                }
                record.state = TxnState.ABORTED
                count += 1
            }
        }
        return count
    }
}
