package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals

// RaftApplier が log index を version として伝えることを確認する
class RaftApplierVersionedTest {
    @Test
    fun versionedStateMachineReceivesLogIndex() = runTest {
        val volatileState = RaftVolatileState(commitIndex = 2)
        val log = MemoryRaftLog()
        val stateMachine = RecordingVersionedStateMachine()
        log.append(entry(1))
        log.append(entry(2))

        RaftApplier(volatileState, log, stateMachine).applyCommitted()

        assertEquals(listOf(1L, 2L), stateMachine.versions)
        assertEquals(2L, volatileState.lastApplied)
    }

    private fun entry(index: Long) =
        RaftLogEntry(
            index = index,
            term = 1,
            command = RaftCommand.Put(
                key = byteArrayOf(index.toByte()),
                value = byteArrayOf((index * 10).toByte()),
            ),
        )

    private class RecordingVersionedStateMachine : VersionedRaftStateMachine {
        val versions = mutableListOf<Long>()

        override suspend fun apply(command: RaftCommand) = Unit

        override suspend fun applyAt(command: RaftCommand, version: Long) {
            versions.add(version)
        }
    }
}
