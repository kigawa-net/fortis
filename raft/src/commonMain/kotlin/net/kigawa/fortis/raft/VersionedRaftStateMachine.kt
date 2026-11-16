package net.kigawa.fortis.raft

// version（Raft log index）付き適用に対応した状態機械
interface VersionedRaftStateMachine : RaftStateMachine {
    // 既存実装との互換のためデフォルトでは version なし適用に委譲する
    suspend fun applyAt(command: RaftCommand, version: Long) {
        apply(command)
    }
}
