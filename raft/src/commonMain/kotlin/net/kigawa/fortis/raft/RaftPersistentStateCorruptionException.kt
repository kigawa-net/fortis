package net.kigawa.fortis.raft

class RaftPersistentStateCorruptionException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
