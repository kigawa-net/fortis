package net.kigawa.fortis.raft

interface RaftStateMachine {
    suspend fun apply(command: RaftCommand)
}