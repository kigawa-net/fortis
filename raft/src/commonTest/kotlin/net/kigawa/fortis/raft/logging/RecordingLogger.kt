package net.kigawa.fortis.raft.logging

/** テスト用: 全ログをメモリに保持する [RaftLogger]。 */
class RecordingLogger : RaftLogger {
    val entries = mutableListOf<StructuredLog>()

    override fun log(
        level: LogLevel,
        component: String,
        event: String,
        fields: Map<String, String>,
    ) {
        entries.add(StructuredLog(level, component, event, fields))
    }
}
