package net.kigawa.fortis.raft.txn

import net.kigawa.fortis.raft.metrics.FortisMetrics
import net.kigawa.fortis.raft.metrics.RaftMetrics
import net.kigawa.fortis.storage.engine.ByteArrayKey

/**
 * 2PC のコーディネーター。
 *
 * スナップショット分離で読み、prepare（全参加者）→全 OK なら commit、
 * 1つでも NG なら abort する。単一グループも参加者1名の 2PC として扱う。
 * 記録はインメモリのみであり、Raft ログへの永続化は将来課題とする。
 *
 * 注意: 本実装は非本番シミュレーションである。確定判断の耐久化がなく、
 * グループへの適用は逐次（A→B の順）で行われるため、途中の失敗やクラッシュでは
 * 一部グループのみ可視という部分的適用が起こり得る。[recover] の
 * presumed-abort も適用済みグループを巻き戻せない。本番利用には耐久化された
 * 確定判断（Raft ログ経由）、冪等な参加者回復、原子可視性の実装が必要。
 *
 * @param metrics 非 null なら commit/abort の確定時に
 * [RaftMetrics.TXN_COMMITTED]/[RaftMetrics.TXN_ABORTED] を加算する。
 */
class TxnCoordinator(
    participants: Map<String, TxnParticipant>,
    private val idGenerator: TxnIdGenerator = TxnIdGenerator(),
    private val metrics: FortisMetrics? = null,
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
            val reads = record.readSet.getOrPut(groupId) { mutableMapOf() }
            if (!reads.containsKey(wrapped)) {
                reads[wrapped] = observed?.copyOf()
            }
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
         * 参加者への適用は逐次であり、途中失敗時は未適用の参加者を abort して
         * 例外を再送出する。適用済み参加者の巻き戻しは行わない（非原子性に
         * ついてはクラス KDoc を参照）。
         *
         * @return commit 成功時 true、競合等で abort 時 false。
         * @throws Throwable 適用中の失敗時。ABORTED を記録して再送出する。
         */
        suspend fun commit(): Boolean {
            val groups = record.writeGroups().toList()
            if (groups.isEmpty()) {
                record.state = TxnState.COMMITTED
                metrics?.incrementCounter(RaftMetrics.TXN_COMMITTED)
                return true
            }
            record.state = TxnState.PREPARING
            val prepared = mutableListOf<TxnParticipant>()
            for (groupId in groups) {
                val participant = requireParticipant(groupId)
                if (!participant.prepare(record)) {
                    for (done in prepared) done.abort(record)
                    record.state = TxnState.ABORTED
                    metrics?.incrementCounter(RaftMetrics.TXN_ABORTED)
                    return false
                }
                prepared.add(participant)
            }
            record.state = TxnState.PREPARED
            val committed = mutableListOf<TxnParticipant>()
            try {
                for (participant in prepared) {
                    participant.commit(record)
                    committed.add(participant)
                }
            } catch (cause: Throwable) {
                for (participant in prepared) {
                    if (participant !in committed) participant.abort(record)
                }
                record.state = TxnState.ABORTED
                metrics?.incrementCounter(RaftMetrics.TXN_ABORTED)
                throw cause
            }
            record.state = TxnState.COMMITTED
            metrics?.incrementCounter(RaftMetrics.TXN_COMMITTED)
            return true
        }

        /** 明示的に破棄する。 */
        suspend fun abort() {
            for (groupId in record.writeGroups()) {
                participants[groupId]?.abort(record)
            }
            record.state = TxnState.ABORTED
            metrics?.incrementCounter(RaftMetrics.TXN_ABORTED)
        }

        private fun requireParticipant(groupId: String): TxnParticipant =
            checkNotNull(participants[groupId]) { "unknown group: $groupId" }
    }

    /**
     * PREPARING / PREPARED のまま残った記録を abort する（presumed-abort）。
     *
     * コーディネーター障害後に未確定の記録を解決する手順を表す。
     * 適用済みグループの巻き戻しは行わないため、部分的適用の可能性は残る
     * （クラス KDoc の非原子性の注意を参照）。
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
                metrics?.incrementCounter(RaftMetrics.TXN_ABORTED)
                count += 1
            }
        }
        return count
    }
}
