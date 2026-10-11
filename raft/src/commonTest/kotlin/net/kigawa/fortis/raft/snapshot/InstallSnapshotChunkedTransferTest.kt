package net.kigawa.fortis.raft.snapshot

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

// チャンク分割転送の分割・再構成のふるまいを確認する
class InstallSnapshotChunkedTransferTest {
    private val metadata = SnapshotMetadata(3, 2)

    @Test
    fun chunkedRoundTrip() {
        val data = ByteArray(10) { it.toByte() }
        val chunks = InstallSnapshotChunks.chunked(
            term = 2,
            leaderId = "leader-1",
            metadata = metadata,
            data = data,
            maxChunkBytes = 3,
        )

        assertEquals(4, chunks.size)
        assertEquals(listOf(0L, 3L, 6L, 9L), chunks.map { it.offset })
        assertEquals(listOf(false, false, false, true), chunks.map { it.isLast })

        val reassembler = SnapshotReassembler()
        var completed: ByteArray? = null
        for (chunk in chunks.dropLast(1)) {
            assertNull(reassembler.accept(chunk))
        }
        completed = reassembler.accept(chunks.last())
        assertTrue(reassembler.isComplete)
        assertContentEquals(data, completed)
    }

    @Test
    fun chunkedBoundarySizes() {
        // ちょうど上限: 単一チャンクで終端する
        val exact = InstallSnapshotChunks.chunked(1, "leader-1", metadata, ByteArray(4) { 7 }, 4)
        assertEquals(1, exact.size)
        assertTrue(exact.single().isLast)
        assertEquals(0L, exact.single().offset)

        // 1バイト: 単一チャンクで終端する
        val single = InstallSnapshotChunks.chunked(1, "leader-1", metadata, byteArrayOf(9), 4)
        assertEquals(1, single.size)
        assertTrue(single.single().isLast)

        // 空 data: 終端のみの単一チャンクになる
        val empty = InstallSnapshotChunks.chunked(1, "leader-1", metadata, ByteArray(0), 4)
        assertEquals(1, empty.size)
        assertTrue(empty.single().isLast)
        val reassembler = SnapshotReassembler()
        assertContentEquals(ByteArray(0), reassembler.accept(empty.single()))
    }

    @Test
    fun chunkedRejectsInvalidLimit() {
        assertFailsWith<IllegalArgumentException> {
            InstallSnapshotChunks.chunked(1, "leader-1", metadata, byteArrayOf(1), 0)
        }
        assertFailsWith<IllegalArgumentException> {
            InstallSnapshotChunks.chunked(1, "leader-1", metadata, byteArrayOf(1), -1)
        }
    }

    @Test
    fun chunkRejectsNegativeOffset() {
        assertFailsWith<IllegalArgumentException> {
            InstallSnapshotChunk(1, "leader-1", metadata, -1, byteArrayOf(1), false)
        }
    }

    @Test
    fun reassemblerRejectsOutOfOrderDuplicateAndGap() {
        val data = ByteArray(6) { it.toByte() }
        val chunks = InstallSnapshotChunks.chunked(1, "leader-1", metadata, data, 2)

        // 順序不正: 先頭を飛ばして2片目から受け付ける
        assertFailsWith<IllegalArgumentException> {
            SnapshotReassembler().accept(chunks[1])
        }

        // 重複: 同じ片を2回受け付ける
        val duplicate = SnapshotReassembler()
        assertNull(duplicate.accept(chunks[0]))
        assertFailsWith<IllegalArgumentException> {
            duplicate.accept(chunks[0])
        }

        // 欠落: 2片目を飛ばして3片目を受け付ける
        val gapped = SnapshotReassembler()
        assertNull(gapped.accept(chunks[0]))
        assertFailsWith<IllegalArgumentException> {
            gapped.accept(chunks[2])
        }
    }

    @Test
    fun reassemblerRejectsInconsistentMetadata() {
        val first = InstallSnapshotChunk(1, "leader-1", metadata, 0, byteArrayOf(1), false)
        val reassembler = SnapshotReassembler()
        assertNull(reassembler.accept(first))

        assertFailsWith<IllegalArgumentException> {
            reassembler.accept(
                InstallSnapshotChunk(1, "leader-1", SnapshotMetadata(4, 2), 1, byteArrayOf(2), true),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            reassembler.accept(
                InstallSnapshotChunk(2, "leader-1", metadata, 1, byteArrayOf(2), true),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            reassembler.accept(
                InstallSnapshotChunk(1, "leader-2", metadata, 1, byteArrayOf(2), true),
            )
        }
    }

    @Test
    fun reassemblerRejectsTrailingChunkAfterLast() {
        val reassembler = SnapshotReassembler()
        assertContentEquals(
            byteArrayOf(1),
            reassembler.accept(InstallSnapshotChunk(1, "leader-1", metadata, 0, byteArrayOf(1), true)),
        )
        assertFailsWith<IllegalStateException> {
            reassembler.accept(InstallSnapshotChunk(1, "leader-1", metadata, 1, byteArrayOf(2), true))
        }
    }

    @Test
    fun discardAllowsReuse() {
        val reassembler = SnapshotReassembler()
        assertNull(reassembler.accept(InstallSnapshotChunk(1, "leader-1", metadata, 0, byteArrayOf(1), false)))
        reassembler.discard()
        assertEquals(0, reassembler.expectedOffset)

        val completed = reassembler.accept(
            InstallSnapshotChunk(1, "leader-1", metadata, 0, byteArrayOf(5), true),
        )
        assertContentEquals(byteArrayOf(5), completed)
    }
}
