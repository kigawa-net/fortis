package net.kigawa.fortis.raft.backup

/** backup バイト列の検証失敗を表す。 */
class RaftBackupCorruptionException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
