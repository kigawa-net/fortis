package net.kigawa.fortis.raft.log

import net.kigawa.fortis.raft.RaftCommand

@ConsistentCopyVisibility
data class RaftLogEntry private constructor(
    val index: Long,
    val term: Long,
    val payload: RaftLogEntryPayload,
    private val gcSafePoint: Long?,
) {
    constructor(index: Long, term: Long, payload: RaftLogEntryPayload) : this(index, term, payload, null)

    // GC has its own wire tag. The public payload identifies it as an internal entry with no application command.
    internal val garbageCollectionSafePoint: Long?
        get() = gcSafePoint

    fun copy(
        index: Long = this.index,
        term: Long = this.term,
        payload: RaftLogEntryPayload = this.payload,
    ): RaftLogEntry = RaftLogEntry(index, term, payload, gcSafePoint.takeIf { payload == this.payload })

    override fun hashCode(): Int {
        val entryHash = (31 * index.hashCode() + term.hashCode()) * 31 + payload.hashCode()
        return gcSafePoint?.let { 31 * entryHash + it.hashCode() } ?: entryHash
    }

    override fun toString(): String =
        if (gcSafePoint == null) "RaftLogEntry(index=$index, term=$term, payload=$payload)"
        else "RaftLogEntry(index=$index, term=$term, gcSafePoint=$gcSafePoint)"

    internal companion object {
        fun collectGarbage(index: Long, term: Long, safePoint: Long): RaftLogEntry =
            RaftLogEntry(index, term, RaftLogEntryPayload.NoOp, safePoint)
    }

    constructor(index: Long, term: Long, command: RaftCommand) :
        this(index, term, RaftLogEntryPayload.Command(command))

    /** Application command, or null for a Raft internal entry. */
    val command: RaftCommand?
        get() = (payload as? RaftLogEntryPayload.Command)?.command

    init {
        require(index > 0) {
            "Raft log index must be positive"
        }

        require(term >= 0) {
            "Raft term must not be negative"
        }
    }
}
