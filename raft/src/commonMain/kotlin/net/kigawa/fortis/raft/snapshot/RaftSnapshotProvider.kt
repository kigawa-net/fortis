package net.kigawa.fortis.raft.snapshot

/** 指定した境界の状態を供給する。取得失敗時は転送を中断する。 */
fun interface RaftSnapshotProvider {
    suspend fun snapshotData(metadata: SnapshotMetadata): ByteArray
}
