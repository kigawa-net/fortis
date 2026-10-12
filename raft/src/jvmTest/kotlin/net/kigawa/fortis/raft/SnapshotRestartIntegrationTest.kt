package net.kigawa.fortis.raft

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.log.FileRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.snapshot.InstallSnapshotHandler
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * スナップショット世代の再起動復旧を確認する。
 *
 * 既存の [PersistentRaftRestartIntegrationTest] が通常エントリの復旧を扱うのに対し、
 * こちらはスナップショット適用による圧縮後の状態が再オープンで復元されることを扱う。
 */
class SnapshotRestartIntegrationTest {
    @Test
    fun compactedStateSurvivesRestart() = runTest {
        withDirectory { directory ->
            val statePath = directory.resolve("raft.state")
            val logPath = directory.resolve("raft.log")
            val store = FileRaftPersistentStateStore(statePath.toFortisFile())
            store.save(6, null)
            val log = FileRaftLog.open(logPath.toFortisFile())
            for (index in 1L..3L) {
                log.append(
                    RaftLogEntry(
                        index = index,
                        term = 5,
                        command = RaftCommand.Put(
                            byteArrayOf(index.toByte()),
                            byteArrayOf(10),
                        ),
                    ),
                )
            }

            val persistentState = store.load()
            InstallSnapshotHandler(
                persistentState = persistentState,
                persistentStateStore = store,
                volatileState = RaftVolatileState(),
                log = log,
                timer = RaftTimer.None,
                snapshotApplier = RaftSnapshotApplier.NoOp,
            ).handle(
                InstallSnapshotRequest(
                    term = 6,
                    leaderId = "leader-1",
                    metadata = SnapshotMetadata(3, 6),
                    data = byteArrayOf(9),
                ),
            )

            val reopenedState = FileRaftPersistentStateStore(statePath.toFortisFile()).load()
            val reopenedLog = FileRaftLog.open(logPath.toFortisFile())

            assertEquals(6, reopenedState.currentTerm)
            assertNull(reopenedState.votedFor)
            // 圧縮後の空ログが復元される
            assertEquals(0, reopenedLog.lastIndex())
            // 復旧後のログは新規エントリを受け付けられる。
            // 圧縮境界以降の番号付け（base-index 対応）は将来課題のため、
            // 現状は先頭からの再採番になる。
            reopenedLog.append(
                RaftLogEntry(
                    index = 1,
                    term = 6,
                    command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(20)),
                ),
            )
            assertEquals(1, reopenedLog.lastIndex())
            assertEquals(6, reopenedLog.get(1)?.term)
        }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withDirectory(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-snapshot-restart-test-")
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
}
