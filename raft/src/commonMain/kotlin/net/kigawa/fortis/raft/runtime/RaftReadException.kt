package net.kigawa.fortis.raft.runtime

class RaftReadNotLeaderException : Exception("Only leader can serve a linearizable Raft read")
class RaftReadLeadershipLostException(val term: Long) :
    Exception("Leadership lost before Raft read in term $term completed")
class RaftReadStoppedException : Exception("Raft runtime stopped before read completed")
