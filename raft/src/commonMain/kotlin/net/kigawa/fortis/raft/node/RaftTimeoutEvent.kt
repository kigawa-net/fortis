package net.kigawa.fortis.raft.node

sealed interface RaftTimeoutEvent {
    data object Election : RaftTimeoutEvent
    data object Heartbeat : RaftTimeoutEvent
}
