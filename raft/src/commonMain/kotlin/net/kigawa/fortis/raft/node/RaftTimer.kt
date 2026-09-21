package net.kigawa.fortis.raft.node

fun interface RaftTimer {
    suspend fun reset(event: RaftTimeoutEvent)

    companion object {
        val None = RaftTimer { }
    }
}
