package net.kigawa.fortis.raft.snapshot

/**
 * リーダー側のスナップショットデータ供給処理。
 *
 * Raft ログ層は [data] を不透明バイト列として扱い、生成方法
 * （状態機械の直列化等）は呼び出し側に委ねる。層の責務を混ぜないため、
 * 取得失敗時は例外を投げて複製打ち切りとして扱う。
 * スナップショット自体の永続化とチャンク分割転送は将来課題。
 */
fun interface RaftSnapshotProvider {
    suspend fun snapshotData(metadata: SnapshotMetadata): ByteArray
}
