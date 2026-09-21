package net.kigawa.fortis.raft.log

class RaftLogCorruptionException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
