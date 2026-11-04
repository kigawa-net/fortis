package net.kigawa.fortis.raft.snapshot

/**
 * 永続化スナップショットが破損していることを表す。
 *
 * 部分書き込みや改ざんを黙って初期化せず、例外で失敗させる。
 */
class RaftSnapshotCorruptionException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
