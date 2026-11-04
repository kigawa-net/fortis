package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals

class RaftApplierTest {
    @Test
    fun appliesCommittedEntriesInOrder() = runTest {
        val volatileState = RaftVolatileState(commitIndex = 2)
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        log.append(entry(1))
        log.append(entry(2))

        RaftApplier(volatileState, log, stateMachine).applyCommitted()

        assertEquals(
            listOf<RaftCommand>(command(1), command(2)),
            stateMachine.applied,
        )
        assertEquals(2L, volatileState.lastApplied)
    }

    @Test
    fun repeatedCallDoesNotApplyEntryAgain() = runTest {
        val volatileState = RaftVolatileState(commitIndex = 1)
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        log.append(entry(1))
        val applier = RaftApplier(volatileState, log, stateMachine)

        applier.applyCommitted()
        applier.applyCommitted()

        assertEquals(listOf<RaftCommand>(command(1)), stateMachine.applied)
        assertEquals(1L, volatileState.lastApplied)
    }

    private fun entry(index: Long) =
        RaftLogEntry(
            index = index,
            term = 1,
            command = command(index),
        )

    private fun command(index: Long) =
        RaftCommand.Put(
            key = byteArrayOf(index.toByte()),
            value = byteArrayOf((index * 10).toByte()),
        )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }
}
