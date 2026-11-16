package net.kigawa.fortis.raft.logging

/**
 * Raft 主要イベント名とコンポーネント名の定数。
 *
 * イベント名の文字列リテラル散在を防ぐため、ログ出力時は本オブジェクトの
 * 定数を使うこと。
 */
object RaftLogEvents {
    /** Raft ランタイムのコンポーネント名。 */
    const val COMPONENT_RUNTIME = "raft-runtime"

    /** トランザクションコーディネーターのコンポーネント名。 */
    const val COMPONENT_TXN = "txn-coordinator"

    /** 選挙開始（タイムアウトで候補者に遷移）。 */
    const val ELECTION_STARTED = "election.started"

    /** 選挙終了（投票応答の処理完了、または選挙失敗）。 */
    const val ELECTION_ENDED = "election.ended"

    /** リーダー昇格。 */
    const val LEADER_ELECTED = "leader.elected"

    /** リーダー降格。 */
    const val LEADER_DEMOTED = "leader.demoted"

    /** ハートビート送信。 */
    const val HEARTBEAT_SENT = "heartbeat.sent"

    /** コマンド追加（ローカルログ追記）。 */
    const val COMMAND_APPENDED = "command.appended"

    /** レプリケーション成功。 */
    const val REPLICATION_SUCCEEDED = "replication.succeeded"

    /** レプリケーション失敗。 */
    const val REPLICATION_FAILED = "replication.failed"

    /** InstallSnapshot 送信。 */
    const val INSTALL_SNAPSHOT_SENT = "install_snapshot.sent"

    /** InstallSnapshot 受信。 */
    const val INSTALL_SNAPSHOT_RECEIVED = "install_snapshot.received"

    /** トランザクション commit 成功。 */
    const val TXN_COMMITTED = "txn.committed"

    /** トランザクション abort。 */
    const val TXN_ABORTED = "txn.aborted"
}
