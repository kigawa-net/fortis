package net.kigawa.fortis.raft

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}