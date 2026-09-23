package net.kigawa.fortis.raft

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class FileRaftPersistentStateStoreTest {
    private val codec = RaftPersistentStateCodec()

    @Test
    fun missingFileLoadsInitialState() = runTest {
        withStatePath { path ->
            val state = store(path).load()

            assertEquals(RaftPersistentState(), state)
            assertFalse(Files.exists(path))
        }
    }

    @Test
    fun saveThenLoadRestoresTermAndVote() = runTest {
        withStatePath { path ->
            val store = store(path)

            store.save(3, "node-2")

            assertEquals(RaftPersistentState(3, "node-2"), store.load())
        }
    }

    @Test
    fun newStoreInstanceRestoresState() = runTest {
        withStatePath { path ->
            store(path).save(3, "node-2")

            val restored = store(path).load()

            assertEquals(RaftPersistentState(3, "node-2"), restored)
        }
    }

    @Test
    fun saveNullVoteReplacesPreviousVote() = runTest {
        withStatePath { path ->
            val store = store(path)
            store.save(3, "node-2")

            store.save(4, null)

            assertEquals(RaftPersistentState(4, null), store(path).load())
        }
    }

    @Test
    fun invalidMagicFailsAsCorruption() = runTest {
        withStatePath { path ->
            val bytes = codec.encode(RaftPersistentState(3, "node-2"))
                .also { it[0] = 0 }
            write(path, bytes)

            assertFailsWith<RaftPersistentStateCorruptionException> {
                store(path).load()
            }
        }
    }

    @Test
    fun truncatedStateFailsAsCorruption() = runTest {
        withStatePath { path ->
            val bytes = codec.encode(RaftPersistentState(3, "node-2"))
            write(path, bytes.copyOf(bytes.size - 1))

            assertFailsWith<RaftPersistentStateCorruptionException> {
                store(path).load()
            }
        }
    }

    private fun store(path: Path) = FileRaftPersistentStateStore(
        path.toFortisFile(),
        codec,
    )

    private suspend fun write(path: Path, bytes: ByteArray) {
        withContext(Dispatchers.IO) { Files.write(path, bytes) }
    }

    private fun Path.toFortisFile(): FortisFile = FsPath(
        elements = toAbsolutePath().map { FsPath.Element.Name(it.toString()) },
        isAbsolute = true,
    ).toFile()

    private suspend fun withStatePath(block: suspend (Path) -> Unit) {
        val directory = withContext(Dispatchers.IO) {
            Files.createTempDirectory("fortis-raft-state-test-")
        }
        val path = directory.resolve("raft.state")
        try {
            block(path)
        } finally {
            withContext(NonCancellable + Dispatchers.IO) {
                Files.deleteIfExists(path)
                Files.deleteIfExists(directory)
            }
        }
    }
}
