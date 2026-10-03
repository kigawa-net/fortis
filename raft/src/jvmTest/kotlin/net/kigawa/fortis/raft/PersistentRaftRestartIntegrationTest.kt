package net.kigawa.fortis.raft

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.follower.FollowerNode
import net.kigawa.fortis.raft.log.FileRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class PersistentRaftRestartIntegrationTest {
    @Test
    fun builderRestoresPersistentStateAndLogAfterRestart() = runTest {
        withDirectory { directory ->
            val statePath = directory.resolve("raft.state")
            val logPath = directory.resolve("raft.log")
            FileRaftPersistentStateStore(statePath.toFortisFile())
                .save(3, "node-2")
            FileRaftLog.open(logPath.toFortisFile()).append(
                RaftLogEntry(
                    index = 1,
                    term = 2,
                    command = RaftCommand.Put(
                        byteArrayOf(1),
                        byteArrayOf(10),
                    ),
                ),
            )

            val restoredStore = FileRaftPersistentStateStore(
                statePath.toFortisFile(),
            )
            val restoredLog = FileRaftLog.open(logPath.toFortisFile())
            val node = RaftNodeBuilder(
                nodeId = "node-1",
                peerIds = setOf("node-2", "node-3"),
                persistentStateStore = restoredStore,
                volatileState = RaftVolatileState(),
                log = restoredLog,
                stateMachine = NoOpStateMachine(),
            ).build()

            assertIs<FollowerNode>(node)
            assertEquals(3L, node.persistentState.currentTerm)
            assertEquals("node-2", node.persistentState.votedFor)
            assertEquals(1L, node.log.lastIndex())
            assertEquals(2L, node.log.get(1)?.term)
        }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withDirectory(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-persistent-raft-test-")
        }
        try {
            block(directory)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.deleteIfExists(directory.resolve("raft.log"))
                Files.deleteIfExists(directory.resolve("raft.state"))
                Files.deleteIfExists(directory)
            }
        }
    }

    private class NoOpStateMachine : RaftStateMachine {
        override suspend fun apply(command: RaftCommand) = Unit
    }
}
