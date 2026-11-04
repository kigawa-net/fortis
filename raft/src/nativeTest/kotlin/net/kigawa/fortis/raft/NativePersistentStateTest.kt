@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package net.kigawa.fortis.raft

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.*
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import platform.posix.getenv
import platform.posix.mkdtemp
import platform.posix.rmdir
import kotlin.test.*

class NativePersistentStateTest {
    @Test
    fun atomicStateSaveAndReopenPreservesVoteAndTerm() = runTest {
        withDirectory { dir ->
            val file = Fs.getPath("$dir/state").toFile()
            FileRaftPersistentStateStore(file).save(3, "node-2")
            assertEquals(RaftPersistentState(3, "node-2"), FileRaftPersistentStateStore(file).load())
            FileRaftPersistentStateStore(file).save(4, null)
            assertEquals(RaftPersistentState(4, null), FileRaftPersistentStateStore(file).load())
            assertFailsWith<IllegalArgumentException> { FileRaftPersistentStateStore(file).save(3, null) }
        }
    }

    @Test
    fun temporaryStateIsPromotedWhenCanonicalIsMissing() = runTest {
        withDirectory { dir ->
            val temporary = Fs.getPath("$dir/state.tmp").toFile()
            FortisRaftStateFileAccess.writeAndSync(temporary, RaftPersistentStateCodec().encode(RaftPersistentState(5, "node-3")))
            val file = Fs.getPath("$dir/state").toFile()
            assertEquals(RaftPersistentState(5, "node-3"), FileRaftPersistentStateStore(file).load())
            assertNull(FortisRaftStateFileAccess.read(temporary))
            assertEquals(RaftPersistentState(5, "node-3"), FileRaftPersistentStateStore(file).load())
        }
    }

    @Test
    fun incompleteTemporaryStateCannotResetTerm() = runTest {
        withDirectory { dir ->
            FortisRaftStateFileAccess.writeAndSync(Fs.getPath("$dir/state.tmp").toFile(), byteArrayOf(1, 2))
            val file = Fs.getPath("$dir/state").toFile()
            assertFailsWith<RaftPersistentStateCorruptionException> { FileRaftPersistentStateStore(file).load() }
            assertNull(FortisRaftStateFileAccess.read(file))
        }
    }

    @Test
    fun positionalIoTruncateAndAtomicReplacementOperateOnActualFiles() = runTest {
        withDirectory { dir ->
            val file = Fs.getPath("$dir/state").toFile()
            file.openReadWrite(isCreate = true) { io ->
                assertEquals(3, io.output.writeAt(0, byteArrayOf(1, 2, 3), 0, 3))
                assertEquals(2, io.output.writeAt(1, byteArrayOf(8, 9), 0, 2))
                val bytes = ByteArray(3)
                assertEquals(3, io.input.readAt(0, bytes, 0, 3))
                assertContentEquals(byteArrayOf(1, 8, 9), bytes)
                io.output.truncate(1)
                assertEquals(1L, io.input.size())
                assertEquals(1, io.output.writeAppend(byteArrayOf(7), 0, 1))
                io.output.sync()
            }
            val temporary = Fs.getPath("$dir/state.tmp").toFile()
            FortisRaftStateFileAccess.writeAndSync(temporary, byteArrayOf(5, 6))
            temporary.atomicReplace(file)
            Fs.getPath(dir).toFile().syncDirectory()
            assertContentEquals(byteArrayOf(5, 6), FortisRaftStateFileAccess.read(file))
            assertNull(FortisRaftStateFileAccess.read(temporary))
        }
    }

    private suspend fun withDirectory(block: suspend (String) -> Unit) {
        val base = getenv("TMPDIR")?.toKString() ?: "/tmp"
        val template = "$base/fortis-native-state-XXXXXX".encodeToByteArray() + byteArrayOf(0)
        val dir = template.usePinned { mkdtemp(it.addressOf(0))?.toKString() ?: error("mkdtemp failed") }
        try {
            block(dir)
        } finally {
            withContext(NonCancellable) {
                Fs.getPath("$dir/state").toFile().deleteIfExists()
                Fs.getPath("$dir/state.tmp").toFile().deleteIfExists()
                check(rmdir(dir) == 0) { "Temporary directory cleanup failed" }
            }
        }
    }
}
