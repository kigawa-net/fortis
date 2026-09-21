package net.kigawa.fortis.raft.log

class MemoryRaftLog : RaftLog {
    private val entries =
        mutableListOf<RaftLogEntry>()

    override suspend fun lastIndex(): Long =
        entries.lastOrNull()?.index ?: 0L

    override suspend fun get(
        index: Long,
    ): RaftLogEntry? {
        if (index <= 0) {
            return null
        }

        return entries.getOrNull(
            (index - 1).toInt()
        )
    }

    override suspend fun append(
        entry: RaftLogEntry,
    ) {
        require(
            entry.index == lastIndex() + 1
        ) {
            "Raft log index must be contiguous"
        }

        entries.add(entry)
    }

    override suspend fun truncateFrom(
        index: Long,
    ) {
        require(index > 0)

        while (
            entries.isNotEmpty() &&
            entries.last().index >= index
        ) {
            entries.removeLast()
        }
    }
}