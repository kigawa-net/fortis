package net.kigawa.fortis.raft.vote

internal data class RaftVotingConfiguration(
    val current: Set<String>,
    val next: Set<String>? = null,
) {
    val quorums: List<Set<String>> get() = listOfNotNull(current, next)
    val voters: Set<String> get() = next?.let { current + it } ?: current

    fun hasQuorum(confirmed: Set<String>): Boolean =
        quorums.all { voters -> voters.count { it in confirmed } > voters.size / 2 }
}
