package net.kigawa.fortis.raft

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult
import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

class RaftCommandImmutabilityTest {
    @Test
    fun mutatingConstructorInputsDoesNotChangePut() {
        val key = byteArrayOf(1, 2)
        val value = byteArrayOf(10, 20)
        val command = RaftCommand.Put(key, value)
        val hashBefore = command.hashCode()

        key[0] = 9
        value[1] = 9

        assertContentEquals(byteArrayOf(1, 2), command.key)
        assertContentEquals(byteArrayOf(10, 20), command.value)
        assertEquals(hashBefore, command.hashCode())
        assertEquals(RaftCommand.Put(byteArrayOf(1, 2), byteArrayOf(10, 20)), command)
    }

    @Test
    fun mutatingGetterResultDoesNotChangePut() {
        val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(2))
        val hashBefore = command.hashCode()

        command.key[0] = 9
        command.value[0] = 9

        assertContentEquals(byteArrayOf(1), command.key)
        assertContentEquals(byteArrayOf(2), command.value)
        assertEquals(hashBefore, command.hashCode())
    }

    @Test
    fun mutatingConstructorInputAndGetterDoesNotChangeDelete() {
        val key = byteArrayOf(1, 2)
        val command = RaftCommand.Delete(key)
        val hashBefore = command.hashCode()

        key[1] = 7
        command.key[0] = 7

        assertContentEquals(byteArrayOf(1, 2), command.key)
        assertEquals(hashBefore, command.hashCode())
        assertEquals(RaftCommand.Delete(byteArrayOf(1, 2)), command)
    }

    @Test
    fun copyIsIndependentOfOriginal() {
        val original = RaftCommand.Put(byteArrayOf(1), byteArrayOf(2))
        val copied = original.copy(value = byteArrayOf(3))

        assertContentEquals(byteArrayOf(2), original.value)
        assertContentEquals(byteArrayOf(3), copied.value)
        assertContentEquals(byteArrayOf(1), copied.key)
    }

    @Test
    fun logEntryKeepsCommandContentAfterCallerMutatesInput() = runTest {
        val key = byteArrayOf(1, 2)
        val value = byteArrayOf(10, 20)
        val log = MemoryRaftLog()

        log.append(RaftLogEntry(1, 1, RaftCommand.Put(key, value)))
        key[0] = 9
        value[0] = 9

        val command = assertIs<RaftCommand.Put>(log.get(1)?.command)
        assertContentEquals(byteArrayOf(1, 2), command.key)
        assertContentEquals(byteArrayOf(10, 20), command.value)
    }

    @Test
    fun codecEncodesSameBytesAfterCallerMutatesInput() {
        val key = byteArrayOf(1, 2)
        val value = byteArrayOf(10, 20)
        val codec = RaftLogCodec()
        val entry = RaftLogEntry(1, 1, RaftCommand.Put(key, value))
        val before = codec.encode(entry)

        key[0] = 9
        value[0] = 9

        assertContentEquals(before, codec.encode(entry))
    }

    @Test
    fun executeWritesStorageWithoutSharingCallerArrays() = runTest {
        val key = byteArrayOf(1)
        val value = byteArrayOf(2)
        val command = RaftCommand.Put(key, value)
        val storage = MemoryStorageEngine()

        command.execute(storage)
        key[0] = 9
        value[0] = 9
        command.key[0] = 9

        assertContentEquals(byteArrayOf(2), storage.get(byteArrayOf(1)))
        assertEquals(null, storage.get(byteArrayOf(9)))
    }

    @Test
    fun decodedCommandDoesNotShareBytesWithEncodedBuffer() {
        val codec = RaftLogCodec()
        val entry = RaftLogEntry(3, 2, RaftCommand.Delete(byteArrayOf(5, 6)))
        val bytes = codec.encode(entry)

        val decoded = assertIs<RaftLogDecodeResult.Success>(codec.decode(bytes)).entry
        bytes.fill(0)

        assertEquals(entry, decoded)
        assertContentEquals(byteArrayOf(5, 6), assertIs<RaftCommand.Delete>(decoded.command).key)
    }
}
