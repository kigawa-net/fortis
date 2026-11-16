package net.kigawa.fortis.raft.logging

/** 何も出力しないロガー。デフォルト無効化用。 */
object NoOpLogger : RaftLogger {
    override fun log(
        level: LogLevel,
        component: String,
        event: String,
        fields: Map<String, String>,
    ) {
        // no-op
    }
}
