package net.kigawa.fortis.raft.runtime

class RaftProposalNotLeaderException : Exception("Only leader can accept a Raft proposal")

class RaftProposalLeadershipLostException(
    val index: Long,
    val term: Long,
) : Exception("Leadership lost before Raft proposal $index in term $term completed")

class RaftProposalStoppedException : Exception("Raft runtime stopped before proposal completed")
