package net.kigawa.fortis.raft.batch

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.backup.RaftBackupCodec
import net.kigawa.fortis.raft.backup.RaftBackupProcedures
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.log.codec.RaftLogCodec
import net.kigawa.fortis.raft.log.codec.RaftLogDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

class ConditionalBatchRequestTest {
    private val k1 = byteArrayOf(1)
    private val k2 = byteArrayOf(2)
    private val v1 = byteArrayOf(10)

    @Test
    fun blankIdentityIsRejected() {
        assertFailsWith<IllegalArgumentException> { request(principal = "") }
        assertFailsWith<IllegalArgumentException> { request(requestId = "") }
    }

    @Test
    fun batchNeedsAtLeastOneMutation() {
        assertFailsWith<IllegalArgumentException> { request(mutations = emptyList()) }
    }

    @Test
    fun duplicateConditionKeyIsRejected() {
        assertFailsWith<IllegalArgumentException> {
            request(conditions = listOf(KvCondition.Absent(k1), KvCondition.RevisionEquals(k1, 0)))
        }
    }

    @Test
    fun duplicateMutationKeyIsRejected() {
        assertFailsWith<IllegalArgumentException> {
            request(mutations = listOf(KvMutation.Put(k1, v1), KvMutation.Delete(k1)))
        }
    }

    @Test
    fun keyAndValueLimitsAreEnforced() {
        assertFailsWith<IllegalArgumentException> { request(mutations = listOf(KvMutation.Delete(ByteArray(0)))) }
        assertFailsWith<IllegalArgumentException> {
            request(mutations = listOf(KvMutation.Delete(ByteArray(ConditionalBatchRequest.MAX_KEY_BYTES + 1))))
        }
        assertFailsWith<IllegalArgumentException> {
            request(mutations = listOf(KvMutation.Put(k1, ByteArray(ConditionalBatchRequest.MAX_VALUE_BYTES + 1))))
        }
        assertFailsWith<IllegalArgumentException> {
            request(mutations = List(ConditionalBatchRequest.MAX_ITEMS + 1) { KvMutation.Delete(byteArrayOf(it.toByte())) })
        }
    }

    @Test
    fun negativeRevisionIsRejected() {
        assertFailsWith<IllegalArgumentException> { KvCondition.RevisionEquals(k1, -1) }
    }

    @Test
    fun inputArraysAreDefensivelyCopied() {
        val key = byteArrayOf(1)
        val value = byteArrayOf(10)
        val built = request(mutations = listOf(KvMutation.Put(key, value)))
        val digest = built.requestDigest

        key[0] = 9
        value[0] = 9

        assertContentEquals(digest, built.requestDigest)
        assertEquals(KvMutation.Put(byteArrayOf(1), byteArrayOf(10)), built.mutations.single())
    }

    @Test
    fun digestIsCanonicalAndCoversConditionsAndMutationsOnly() {
        val first = request(principal = "a", requestId = "r1", mutations = listOf(KvMutation.Put(k1, v1)))
        val second = request(principal = "b", requestId = "r2", mutations = listOf(KvMutation.Put(k1, v1)))
        val different = request(principal = "a", requestId = "r1", mutations = listOf(KvMutation.Put(k1, byteArrayOf(11))))

        assertContentEquals(first.requestDigest, second.requestDigest)
        assertNotEquals(first.requestDigest.toList(), different.requestDigest.toList())
    }

    @Test
    fun wireCodecRoundTripsRequest() {
        val original = request(
            conditions = listOf(KvCondition.Absent(k1), KvCondition.RevisionEquals(k2, 7)),
            mutations = listOf(KvMutation.Put(k1, v1), KvMutation.Delete(k2)),
        )

        val decoded = ConditionalBatchCodec.decode(ConditionalBatchCodec.encode(original))

        assertEquals(original, decoded)
        assertContentEquals(original.requestDigest, decoded.requestDigest)
    }

    @Test
    fun wireCodecRejectsTruncatedTrailingAndNonCanonicalInput() {
        val encoded = ConditionalBatchCodec.encode(request(mutations = listOf(KvMutation.Put(k1, v1))))

        assertFailsWith<IllegalArgumentException> { ConditionalBatchCodec.decode(encoded.copyOf(encoded.size - 1)) }
        assertFailsWith<IllegalArgumentException> { ConditionalBatchCodec.decode(encoded + byteArrayOf(0)) }
        // 正規形本体の先頭（version バイト）を壊す。
        // 構成: principal(4+14) + requestId(4+2) + body長(4) の直後が body[0]
        val bodyVersionOffset = 4 + "tenant-a/alice".length + 4 + "r1".length + 4
        val corrupted = encoded.copyOf().also { it[bodyVersionOffset] = 2 }
        assertFailsWith<IllegalArgumentException> { ConditionalBatchCodec.decode(corrupted) }
    }

    @Test
    fun logCodecRoundTripsConditionalBatchEntry() {
        val entry = RaftLogEntry(
            index = 5,
            term = 2,
            command = RaftCommand.ConditionalBatch(
                request(conditions = listOf(KvCondition.Absent(k1)), mutations = listOf(KvMutation.Put(k1, v1))),
            ),
        )
        val codec = RaftLogCodec()

        val encoded = codec.encode(entry)
        val result = assertIs<RaftLogDecodeResult.Success>(codec.decode(encoded))

        assertEquals(entry, result.entry)
        assertEquals(encoded.size, result.bytesRead)
    }

    @Test
    fun rpcCodecRoundTripsConditionalBatchEntry() {
        val entry = RaftLogEntry(
            index = 2,
            term = 3,
            command = RaftCommand.ConditionalBatch(request(mutations = listOf(KvMutation.Delete(k2)))),
        )
        val message = RaftRpcMessage.AppendEntries(
            AppendEntriesRequest(3, "node-1", 1, 1, listOf(entry), 1),
        )
        val codec = RaftRpcCodec()

        val encoded = codec.encode(message)
        val result = assertIs<RaftRpcDecodeResult.Success>(codec.decode(encoded))

        assertEquals(message, result.message)
    }

    @Test
    fun backupRestoreReplaysConditionalBatchesIntoSameReceipts() = runTest {
        val log = MemoryRaftLog()
        val batches = listOf(
            request(requestId = "r1", conditions = listOf(KvCondition.Absent(k1)), mutations = listOf(KvMutation.Put(k1, v1))),
            request(requestId = "r2", conditions = listOf(KvCondition.Absent(k1)), mutations = listOf(KvMutation.Put(k2, v1))),
        )
        batches.forEachIndexed { offset, batch ->
            log.append(RaftLogEntry(offset + 1L, 1, RaftCommand.ConditionalBatch(batch)))
        }
        val store = MemoryRaftPersistentStateStore(RaftPersistentState(1, "node-2"))
        val backupCodec = RaftBackupCodec()

        val restoredBackup = backupCodec.import(
            backupCodec.export(RaftBackupProcedures.backup(log, store, upToIndex = 2)),
        )
        val targetLog = MemoryRaftLog()
        RaftBackupProcedures.restore(restoredBackup, targetLog, MemoryRaftPersistentStateStore())

        val original = StorageEngineStateMachine(MemoryMvccStorageEngine())
        val restored = StorageEngineStateMachine(MemoryMvccStorageEngine())
        (1L..2L).forEach { index ->
            original.applyAt(log.get(index)!!.command!!, index)
            restored.applyAt(targetLog.get(index)!!.command!!, index)
        }

        assertEquals(original.receipts.entries(), restored.receipts.entries())
        assertContentEquals(original.snapshotData(), restored.snapshotData())
    }

    private fun request(
        principal: String = "tenant-a/alice",
        requestId: String = "r1",
        conditions: List<KvCondition> = emptyList(),
        mutations: List<KvMutation> = listOf(KvMutation.Put(k1, v1)),
    ) = ConditionalBatchRequest(principal, requestId, conditions, mutations)
}
