package net.kigawa.fortis.raft.logging

/**
 * 構造化ログエントリ。
 *
 * 出力先（sink）は呼び出し側が注入する [RaftLogger] が決定する。
 * 本クラスはデータのみを持ち、出力処理は行わない。
 *
 * @param level ログレベル。
 * @param component 出力元コンポーネント名（例: [RaftLogEvents.COMPONENT_RUNTIME]）。
 * @param event イベント名（[RaftLogEvents] の定数を使うこと）。
 * @param fields キー・バリューのペア。値は文字列として保持する。
 */
data class StructuredLog(
    val level: LogLevel,
    val component: String,
    val event: String,
    val fields: Map<String, String> = emptyMap(),
)
