package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.io.fs.*
import net.kigawa.fortis.raft.RaftStateFileAccess
import kotlin.test.*

class SnapshotPublicationTest {
    private val file = Fs.getPath("snapshot").toFile()

    @Test
    fun completePublicationSurvivesPowerLoss() = runTest {
        val fs = Filesystem()
        replaceSnapshotFile(file, byteArrayOf(2, 3), fs)
        assertEquals(listOf("writeSync", "rename", "directorySync"), fs.events)
        fs.powerLoss()
        assertContentEquals(byteArrayOf(2, 3), fs.read(file))
    }

    @Test
    fun partialTemporaryWriteLeavesAcknowledgedSnapshotIntact() = runTest {
        val fs = Filesystem("writeSync")
        assertFailsWith<IllegalStateException> { replaceSnapshotFile(file, byteArrayOf(2, 3), fs) }
        fs.powerLoss()
        assertContentEquals(byteArrayOf(1), fs.read(file))
    }

    @Test
    fun renameFailureLeavesAcknowledgedSnapshotIntact() = runTest {
        val fs = Filesystem("rename")
        assertFailsWith<IllegalStateException> { replaceSnapshotFile(file, byteArrayOf(2, 3), fs) }
        fs.powerLoss()
        assertContentEquals(byteArrayOf(1), fs.read(file))
    }

    @Test
    fun failedDirectorySyncNeverAcknowledgesNewSnapshot() = runTest {
        val fs = Filesystem("directorySync")
        assertFailsWith<IllegalStateException> { replaceSnapshotFile(file, byteArrayOf(2, 3), fs) }
        assertContentEquals(byteArrayOf(2, 3), fs.read(file))
        fs.powerLoss()
        assertContentEquals(byteArrayOf(1), fs.read(file))
    }

    private inner class Filesystem(private val failure: String? = null) : RaftStateFileAccess {
        private var durable = mutableMapOf(file to byteArrayOf(1))
        private var visible = durable.toMutableMap()
        val events = mutableListOf<String>()
        override suspend fun read(file: FortisFile): ByteArray? = visible[file]?.copyOf()
        override suspend fun writeAndSync(file: FortisFile, data: ByteArray) {
            events.add("writeSync")
            if (failure == "writeSync") {
                visible[file] = data.copyOf(1)
                error("Interrupted temporary write")
            }
            visible[file] = data.copyOf()
        }
        override suspend fun atomicReplace(source: FortisFile, target: FortisFile) {
            events.add("rename")
            check(failure != "rename")
            visible[target] = visible.remove(source)!!
        }
        override suspend fun syncDirectory(directory: FortisFile) {
            events.add("directorySync")
            check(failure != "directorySync")
            durable = visible.toMutableMap()
        }
        override suspend fun syncFile(file: FortisFile) {}
        override suspend fun deleteIfExists(file: FortisFile) { visible.remove(file) }
        fun powerLoss() { visible = durable.toMutableMap() }
    }
}
