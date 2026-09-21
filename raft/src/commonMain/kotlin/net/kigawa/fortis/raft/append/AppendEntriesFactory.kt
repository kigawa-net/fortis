package net.kigawa.fortis.raft.append

import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftRole
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState

class AppendEntriesFactory(
    private val nodeId: String,
    private val state: RaftPersistentState,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
) {
    suspend fun create(
        peerId: String, role: RaftRole, peers: Map<String, RaftPeerProgress>,
    ): AppendEntriesRequest {
        check(role == RaftRole.LEADER) {
            "Only leader can create AppendEntries"
        }

        val progress =
            requireNotNull(peers[peerId]) {
                "Unknown peer: $peerId"
            }

        return createInternal(progress, includeEntries = true)
    }

    suspend fun createHeartbeat(
        peerId: String,
        role: RaftRole,
        peers: Map<String, RaftPeerProgress>,
    ): AppendEntriesRequest {
        check(role == RaftRole.LEADER) {
            "Only leader can create heartbeat"
        }
        val progress = requireNotNull(peers[peerId]) {
            "Unknown peer: $peerId"
        }
        return createInternal(progress, includeEntries = false)
    }

    private suspend fun createInternal(
        progress: RaftPeerProgress,
        includeEntries: Boolean,
    ): AppendEntriesRequest {
        val prevLogIndex = progress.nextIndex - 1
        val prevLogTerm =
            if (prevLogIndex == 0L) {
                0L
            } else {
                log.get(prevLogIndex)?.term
                    ?: error("Missing previous log entry")
            }

        val entries = mutableListOf<RaftLogEntry>()

        if (includeEntries) {
            var index = progress.nextIndex
            val lastIndex = log.lastIndex()

            while (index <= lastIndex) {
                entries += requireNotNull(log.get(index))
                index++
            }
        }

        return AppendEntriesRequest(
            term = state.currentTerm,
            leaderId = nodeId,
            prevLogIndex = prevLogIndex,
            prevLogTerm = prevLogTerm,
            entries = entries,
            leaderCommit = volatileState.commitIndex,
        )
    }
}
