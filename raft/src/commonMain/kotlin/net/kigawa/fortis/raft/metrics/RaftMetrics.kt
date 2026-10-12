package net.kigawa.fortis.raft.metrics

/** メトリクス名の定数集。文字列リテラルの散在を防ぐ。 */
object RaftMetrics {
    /** 適用済みログエントリ数（カウンタ）。 */
    const val APPLIED_ENTRIES = "raft.applied_entries"

    /** 受理した提案数（カウンタ）。 */
    const val PROPOSALS_SUBMITTED = "raft.proposals_submitted"

    /** ReadIndex 要求数（カウンタ）。 */
    const val READ_INDEX_REQUESTS = "raft.read_index_requests"

    /** 確定したトランザクション数（カウンタ）。 */
    const val TXN_COMMITTED = "raft.txn_committed"

    /** 中断したトランザクション数（カウンタ）。 */
    const val TXN_ABORTED = "raft.txn_aborted"

    /** 取得したスナップショット数（カウンタ）。 */
    const val SNAPSHOTS_TAKEN = "raft.snapshots_taken"

    /** ログ圧縮の実行回数（カウンタ）。 */
    const val COMPACTIONS = "raft.compactions"

    /** 現在のターム（ゲージ）。 */
    const val CURRENT_TERM = "raft.current_term"

    /** 確定インデックス（ゲージ）。 */
    const val COMMIT_INDEX = "raft.commit_index"

    /** 適用済みインデックス（ゲージ）。 */
    const val LAST_APPLIED = "raft.last_applied"
}
