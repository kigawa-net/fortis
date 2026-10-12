package net.kigawa.fortis.raft.node

fun interface RaftTimer {
    suspend fun reset(event: RaftTimeoutEvent)

    suspend fun cancel() = Unit

    companion object {
        val None = RaftTimer { }
    }
}
