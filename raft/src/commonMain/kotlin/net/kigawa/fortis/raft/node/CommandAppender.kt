package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftCommitAdvancer
import net.kigawa.fortis.raft.RaftCommitQuorum
import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.RaftLogEntryPayload

class CommandAppender(
    private val persistentState: RaftPersistentState,
    private val log: RaftLog,
    private val commitAdvancer: RaftCommitAdvancer,
    private val applier: RaftApplier,
) {
    suspend fun append(
        command: RaftCommand,
        peers: Collection<RaftPeerProgress>,
    ): RaftLogEntry = append(RaftLogEntryPayload.Command(command), peers)

    suspend fun append(
        payload: RaftLogEntryPayload,
        peers: Collection<RaftPeerProgress>,
    ): RaftLogEntry = append(payload) { listOf(RaftCommitQuorum(peers)) }

    internal suspend fun append(
        payload: RaftLogEntryPayload,
        quorumProgress: suspend () -> List<RaftCommitQuorum>,
    ): RaftLogEntry {
        val entry = RaftLogEntry(
            index = log.lastIndex() + 1,
            term = persistentState.currentTerm,
            payload = payload,
        )
        log.append(entry)

        commitAdvancer.advanceQuorums(quorumProgress())
        applier.applyCommitted()
        return entry
    }
}
