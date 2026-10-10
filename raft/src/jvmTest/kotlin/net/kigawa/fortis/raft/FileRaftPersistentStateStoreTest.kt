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

    @Test
    fun completeTemporaryOnlyStateIsRecovered() = runTest {
        withStatePath { path ->
            val temp = path.resolveSibling("raft.state.tmp")
            write(temp, codec.encode(RaftPersistentState(7, "node-2")))
            assertEquals(RaftPersistentState(7, "node-2"), store(path).load())
            assertFalse(Files.exists(temp))
            assertEquals(RaftPersistentState(7, "node-2"), store(path).load())
        }
    }

    @Test
    fun incompleteTemporaryOnlyStateFailsClosed() = runTest {
        withStatePath { path ->
            write(path.resolveSibling("raft.state.tmp"), byteArrayOf(1, 2))
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).load() }
            assertFalse(Files.exists(path))
        }
    }

    @Test
    fun canonicalStateWinsOverCompleteOrIncompleteTemporaryFile() = runTest {
        withStatePath { path ->
            store(path).save(3, "node-2")
            val temp = path.resolveSibling("raft.state.tmp")
            write(temp, codec.encode(RaftPersistentState(4, "node-3")))
            assertEquals(RaftPersistentState(3, "node-2"), store(path).load())
            assertFalse(Files.exists(temp))
            write(temp, byteArrayOf(1, 2))
            assertEquals(RaftPersistentState(3, "node-2"), store(path).load())
            assertFalse(Files.exists(temp))
        }
    }

    @Test
    fun corruptCanonicalStateDoesNotFallBackToTemporaryFile() = runTest {
        withStatePath { path ->
            write(path, byteArrayOf(1, 2))
            write(path.resolveSibling("raft.state.tmp"), codec.encode(RaftPersistentState(4, "node-3")))
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).load() }
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).save(5, null) }
        }
    }

    @Test
    fun termAndVoteCannotRegressAfterReopening() = runTest {
        withStatePath { path ->
            store(path).save(7, "node-2")
            assertFailsWith<IllegalArgumentException> { store(path).save(6, null) }
            assertFailsWith<IllegalArgumentException> { store(path).save(7, "node-3") }
            assertFailsWith<IllegalArgumentException> { store(path).save(7, null) }
            assertEquals(RaftPersistentState(7, "node-2"), store(path).load())
        }
    }

    @Test
    fun legacyCanonicalRecordMigratesOnNextSave() = runTest {
        withStatePath { path ->
            write(path, RaftPersistentStateCodec(version = 1).encode(RaftPersistentState(3, "node-2")))
            assertEquals(RaftPersistentState(3, "node-2"), store(path).load())
            store(path).save(4, null)
            assertEquals(2.toByte(), Files.readAllBytes(path)[4])
            assertEquals(RaftPersistentState(4, null), store(path).load())
        }
    }

    @Test
    fun sameLengthTermCorruptionFailsClosed() = runTest {
        withStatePath { path ->
            store(path).save(7, "node-2")
            write(path, Files.readAllBytes(path).also { it[12] = 6 })
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).load() }
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).save(8, null) }
        }
    }

    @Test
    fun unchecksummedTemporaryOnlyStateIsNotPromoted() = runTest {
        withStatePath { path ->
            write(path.resolveSibling("raft.state.tmp"), RaftPersistentStateCodec(version = 1).encode(RaftPersistentState(3, "node-2")))
            assertFailsWith<RaftPersistentStateCorruptionException> { store(path).load() }
            assertFalse(Files.exists(path))
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
                Files.deleteIfExists(path.resolveSibling("raft.state.tmp"))
                Files.deleteIfExists(directory)
            }
        }
    }
}
