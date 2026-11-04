package net.kigawa.fortis.raft.runtime

/** Applied commit barrier obtained from a fresh quorum confirmation in [term]. */
data class RaftReadIndex(val index: Long, val term: Long)
