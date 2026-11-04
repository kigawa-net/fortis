package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.RaftApplier
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftCommitAdvancer
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
    ): RaftLogEntry {
        val entry = RaftLogEntry(
            index = log.lastIndex() + 1,
            term = persistentState.currentTerm,
            payload = payload,
        )
        log.append(entry)

        commitAdvancer.advance(peers)
        applier.applyCommitted()
        return entry
    }
}
