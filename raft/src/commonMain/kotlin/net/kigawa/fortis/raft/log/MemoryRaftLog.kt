package net.kigawa.fortis.raft.log

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class MemoryRaftLog : RaftLog {
    private val entries =
        mutableListOf<RaftLogEntry>()
    private val mutex = Mutex()

    override suspend fun lastIndex(): Long =
        mutex.withLock {
            entries.lastOrNull()?.index ?: 0L
        }
    override suspend fun get(
        index: Long,
    ): RaftLogEntry? =
        mutex.withLock {
            if (index <= 0 || index > Int.MAX_VALUE) {
                return@withLock null
            }

            entries.getOrNull(
                (index - 1).toInt()
            )
        }
    override suspend fun append(
        entry: RaftLogEntry,
    ): Unit = mutex.withLock {
        val lastIndex =
            entries.lastOrNull()?.index ?: 0L

        require(entry.index == lastIndex + 1) {
            "Raft log index must be contiguous"
        }

        entries.add(entry)
    }
    override suspend fun truncateFrom(
        index: Long,
    ) = mutex.withLock {
        require(index > 0)

        while (
            entries.isNotEmpty() &&
            entries.last().index >= index
        ) {
            entries.removeLast()
        }
    }
}