package net.kigawa.fortis.raft.logging

/**
 * Raft 構造化ログの出力先インタフェース。
 *
 * 実装は呼び出し側が注入する。何も出力しない [NoOpLogger] が
 * デフォルトとして使われる。
 */
interface RaftLogger {
    /** 構造化ログを1件出力する。 */
    fun log(
        level: LogLevel,
        component: String,
        event: String,
        fields: Map<String, String> = emptyMap(),
    )

    /** DEBUG ログを出力する。 */
    fun debug(
        component: String,
        event: String,
        fields: Map<String, String> = emptyMap(),
    ) = log(LogLevel.DEBUG, component, event, fields)

    /** INFO ログを出力する。 */
    fun info(
        component: String,
        event: String,
        fields: Map<String, String> = emptyMap(),
    ) = log(LogLevel.INFO, component, event, fields)

    /** WARN ログを出力する。 */
    fun warn(
        component: String,
        event: String,
        fields: Map<String, String> = emptyMap(),
    ) = log(LogLevel.WARN, component, event, fields)

    /** ERROR ログを出力する。 */
    fun error(
        component: String,
        event: String,
        fields: Map<String, String> = emptyMap(),
    ) = log(LogLevel.ERROR, component, event, fields)
}
