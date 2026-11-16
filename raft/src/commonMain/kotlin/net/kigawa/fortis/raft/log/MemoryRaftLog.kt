package net.kigawa.fortis.raft.log

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

import net.kigawa.fortis.raft.snapshot.SnapshotMetadata

class MemoryRaftLog : RaftLog {
    private var base: SnapshotMetadata? = null
    private val entries =
        mutableListOf<RaftLogEntry>()
    private val mutex = Mutex()

    override suspend fun snapshotMetadata(): SnapshotMetadata? = mutex.withLock { base }

    override suspend fun compactThrough(metadata: SnapshotMetadata, appliedIndex: Long) = mutex.withLock {
        require(appliedIndex >= metadata.lastIncludedIndex) { "Snapshot is not applied" }
        val previous = base
        require(metadata.lastIncludedIndex >= (previous?.lastIncludedIndex ?: 0))
        if (metadata == previous) return@withLock
        require(base?.lastIncludedIndex != metadata.lastIncludedIndex) { "Snapshot boundary term mismatch" }
        val matches = entries.any { it.index == metadata.lastIncludedIndex && it.term == metadata.lastIncludedTerm }
        entries.removeAll { !matches || it.index <= metadata.lastIncludedIndex }
        base = metadata
    }

    override suspend fun lastIndex(): Long =
        mutex.withLock {
            entries.lastOrNull()?.index ?: base?.lastIncludedIndex ?: 0L
        }
    override suspend fun get(
        index: Long,
    ): RaftLogEntry? =
        mutex.withLock {
            val relative = index - (base?.lastIncludedIndex ?: 0L) - 1
            if (relative < 0 || relative > Int.MAX_VALUE) {
                return@withLock null
            }

            entries.getOrNull(
                relative.toInt()
            )
        }
    override suspend fun append(
        entry: RaftLogEntry,
    ): Unit = mutex.withLock {
        val lastIndex =
            entries.lastOrNull()?.index ?: base?.lastIncludedIndex ?: 0L

        require(entry.index == lastIndex + 1) {
            "Raft log index must be contiguous"
        }

        entries.add(entry)
    }
    override suspend fun truncateFrom(
        index: Long,
    ) = mutex.withLock {
        require(index > (base?.lastIncludedIndex ?: 0))

        while (
            entries.isNotEmpty() &&
            entries.last().index >= index
        ) {
            entries.removeLast()
        }
    }
}