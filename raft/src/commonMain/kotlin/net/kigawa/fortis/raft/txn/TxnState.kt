package net.kigawa.fortis.raft.txn

/**
 * インメモリのトランザクション記録状態。
 *
 * 永続ログを持たないため、Raft ログへの永続化は将来課題とする。
 * コーディネーター障害時は COMMITTED 記録がなければ abort する
 * presumed-abort で回復する。
 */
enum class TxnState {
    /** 検証・ステージング中。 */
    PREPARING,

    /** 全参加者が検証済み。障害時は presumed-abort の対象。 */
    PREPARED,

    /** 全参加者に確定適用済み。 */
    COMMITTED,

    /** 破棄済み。 */
    ABORTED,
}
