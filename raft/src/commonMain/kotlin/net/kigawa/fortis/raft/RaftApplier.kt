package net.kigawa.fortis.raft

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.log.RaftLogEntryPayload
import net.kigawa.fortis.raft.metrics.FortisMetrics
import net.kigawa.fortis.raft.metrics.RaftMetrics
import net.kigawa.fortis.raft.vote.RaftVolatileState

/**
 * コミット済みエントリを状態機械へ適用する。
 *
 * @param metrics 非 null なら適用成功ごとに [RaftMetrics.APPLIED_ENTRIES] を
 * 加算し、[RaftMetrics.LAST_APPLIED] を更新する。
 */
class RaftApplier(
    val volatileState: RaftVolatileState,
    val log: RaftLog,
    val stateMachine: RaftStateMachine,
    val metrics: FortisMetrics? = null,
) {
    private val mutex = Mutex()

    suspend fun applyCommitted(): Unit = mutex.withLock {
        while (volatileState.lastApplied < volatileState.commitIndex) {
            val index = volatileState.lastApplied + 1
            val entry = checkNotNull(log.get(index)) {
                "Committed Raft log entry is missing at index $index"
            }

            when (val payload = entry.payload) {
                is RaftLogEntryPayload.Command -> {
                    if (stateMachine is VersionedRaftStateMachine) {
                        stateMachine.applyAt(payload.command, index)
                    } else {
                        stateMachine.apply(payload.command)
                    }
                }
                RaftLogEntryPayload.NoOp -> Unit
            }
            volatileState.lastApplied = index
            metrics?.incrementCounter(RaftMetrics.APPLIED_ENTRIES)
            metrics?.setGauge(RaftMetrics.LAST_APPLIED, index)
        }
    }
}
