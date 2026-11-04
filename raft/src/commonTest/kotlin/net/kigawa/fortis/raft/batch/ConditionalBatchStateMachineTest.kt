package net.kigawa.fortis.raft.batch

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.StorageEngineStateMachine
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class ConditionalBatchStateMachineTest {
    private val k1 = byteArrayOf(1)
    private val k2 = byteArrayOf(2)
    private val k3 = byteArrayOf(3)
    private val v1 = byteArrayOf(10)
    private val v2 = byteArrayOf(20)
    private val w = byteArrayOf(30)

    @Test
    fun satisfiedConditionsApplyAllMutationsAtomicallyAtLogIndex() = runTest {
        val machine = newMachine()

        machine.applyAt(
            batch("r1", listOf(KvCondition.Absent(k1), KvCondition.Absent(k2)), listOf(KvMutation.Put(k1, v1), KvMutation.Put(k2, v2))),
            1L,
        )

        assertEquals(BatchOutcome.Applied(1L), receipt(machine, "r1").outcome)
        assertContentEquals(v1, machine.get(k1))
        assertContentEquals(v2, machine.get(k2))
    }

    @Test
    fun failedConditionRejectsWithoutApplyingAnyMutation() = runTest {
        val machine = newMachine()
        machine.applyAt(RaftCommand.Put(k1, v1), 1L)

        machine.applyAt(
            batch("r1", listOf(KvCondition.Absent(k1), KvCondition.Absent(k3)), listOf(KvMutation.Put(k2, v2), KvMutation.Put(k3, w))),
            2L,
        )

        assertEquals(BatchOutcome.Rejected(failedCondition = 0, observedRevision = 1L), receipt(machine, "r1").outcome)
        assertNull(machine.get(k2))
        assertNull(machine.get(k3))
    }

    @Test
    fun laterConditionFailureAlsoRejectsWholeBatch() = runTest {
        val machine = newMachine()
        machine.applyAt(RaftCommand.Put(k1, v1), 1L)

        machine.applyAt(
            batch("r1", listOf(KvCondition.RevisionEquals(k1, 1L), KvCondition.RevisionEquals(k3, 5L)), listOf(KvMutation.Put(k2, v2))),
            2L,
        )

        assertEquals(BatchOutcome.Rejected(failedCondition = 1, observedRevision = 0L), receipt(machine, "r1").outcome)
        assertNull(machine.get(k2))
    }

    @Test
    fun revisionEqualsTracksLastWriteIndexIncludingDeletes() = runTest {
        val machine = newMachine()
        machine.applyAt(batch("put", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))), 1L)
        machine.applyAt(batch("update", listOf(KvCondition.RevisionEquals(k1, 1L)), listOf(KvMutation.Put(k1, v2))), 2L)
        machine.applyAt(batch("stale", listOf(KvCondition.RevisionEquals(k1, 1L)), listOf(KvMutation.Put(k1, w))), 3L)
        machine.applyAt(batch("delete", listOf(KvCondition.RevisionEquals(k1, 2L)), listOf(KvMutation.Delete(k1))), 4L)
        machine.applyAt(batch("absent", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k2, v2))), 5L)

        assertEquals(BatchOutcome.Applied(2L), receipt(machine, "update").outcome)
        assertEquals(BatchOutcome.Rejected(0, observedRevision = 2L), receipt(machine, "stale").outcome)
        assertEquals(BatchOutcome.Applied(4L), receipt(machine, "delete").outcome)
        assertEquals(BatchOutcome.Applied(5L), receipt(machine, "absent").outcome)
        assertNull(machine.get(k1))
        assertContentEquals(v2, machine.get(k2))
    }

    @Test
    fun sameRequestReplayReturnsStoredResultWithoutReapplying() = runTest {
        val machine = newMachine()
        val command = batch("r1", listOf(KvCondition.RevisionEquals(k1, 0L)), listOf(KvMutation.Put(k1, v1)))
        machine.applyAt(command, 1L)

        // 再適用されていれば RevisionEquals(k1, 0) は不成立となり結果が Rejected に変わる
        machine.applyAt(command, 2L)

        assertEquals(BatchOutcome.Applied(1L), receipt(machine, "r1").outcome)
        assertContentEquals(v1, machine.get(k1))
        assertEquals(1, machine.receipts.size)
    }

    @Test
    fun sameRequestIdWithDifferentDigestIsRejectedAndLeavesReceiptUnchanged() = runTest {
        val storage = MemoryMvccStorageEngine()
        val machine = StorageEngineStateMachine(storage)
        machine.applyAt(batch("r1", emptyList(), listOf(KvMutation.Put(k1, v1))), 1L)
        val changed = batch("r1", emptyList(), listOf(KvMutation.Put(k1, v2)))

        val result = ConditionalBatchApplier.apply(changed.request, 2L, storage, machine.receipts)

        assertIs<ApplyResult.DigestMismatch>(result)
        assertEquals(receipt(machine, "r1"), result.existing)
        assertContentEquals(v1, machine.get(k1))
    }

    @Test
    fun requestIdIsScopedByPrincipal() = runTest {
        val machine = newMachine()
        machine.applyAt(batch("r1", emptyList(), listOf(KvMutation.Put(k1, v1))), 1L)

        machine.applyAt(
            batch("r1", emptyList(), listOf(KvMutation.Put(k2, v2)), principal = "tenant-b/bob"),
            2L,
        )

        assertEquals(BatchOutcome.Applied(2L), receipt(machine, "r1", principal = "tenant-b/bob").outcome)
        assertContentEquals(v2, machine.get(k2))
    }

    @Test
    fun concurrentCasOnSameRevisionHasExactlyOneWinner() = runTest {
        val machine = newMachine()
        val requestIds = (1..5).map { "claim-$it" }

        requestIds.forEachIndexed { offset, requestId ->
            machine.applyAt(
                batch(requestId, listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, byteArrayOf((offset + 1).toByte())))),
                offset + 1L,
            )
        }

        val outcomes = requestIds.map { receipt(machine, it).outcome }
        assertEquals(BatchOutcome.Applied(1L), outcomes.first())
        assertEquals(1, outcomes.count { it is BatchOutcome.Applied })
        assertEquals(4, outcomes.count { it is BatchOutcome.Rejected })
        assertContentEquals(byteArrayOf(1), machine.get(k1))
    }

    @Test
    fun snapshotRestoreKeepsReceiptsAndRevisionsSoResendDoesNotDoubleApply() = runTest {
        val source = newMachine()
        source.applyAt(batch("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))), 1L)
        source.applyAt(RaftCommand.Put(k2, v2), 2L)
        source.applyAt(batch("r2", listOf(KvCondition.Absent(k3)), listOf(KvMutation.Put(k3, w))), 3L)
        // 拒否結果もスナップショットに含まれること
        source.applyAt(batch("r3", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k2, w))), 4L)
        val snapshot = source.snapshotData()

        val restored = newMachine()
        restored.restoreSnapshot(snapshot)

        assertEquals(receipt(source, "r1"), receipt(restored, "r1"))
        assertEquals(BatchOutcome.Rejected(0, observedRevision = 1L), receipt(restored, "r3").outcome)
        assertContentEquals(snapshot, restored.snapshotData())

        // 同一要求の再送は保存結果を返し、二重適用しない
        restored.applyAt(batch("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))), 5L)
        assertEquals(BatchOutcome.Applied(1L), receipt(restored, "r1").outcome)
        assertContentEquals(v1, restored.get(k1))

        // 履歴（revision）も復元されていること: k1 の revision は 1 のまま
        restored.applyAt(batch("after", listOf(KvCondition.RevisionEquals(k1, 1L)), listOf(KvMutation.Put(k1, v2))), 6L)
        assertEquals(BatchOutcome.Applied(6L), receipt(restored, "after").outcome)
        assertContentEquals(v2, restored.get(k1))
    }

    @Test
    fun restoreRejectsNonEmptyTarget() = runTest {
        val source = newMachine()
        source.applyAt(RaftCommand.Put(k1, v1), 1L)
        val snapshot = source.snapshotData()
        val target = newMachine()
        target.applyAt(RaftCommand.Put(k2, v2), 1L)

        assertFailsWith<IllegalStateException> { target.restoreSnapshot(snapshot) }
    }

    @Test
    fun reapplyingLogAfterRestartLeavesStateUnchangedAndReplayIsDeterministic() = runTest {
        val log = listOf(
            batch("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))),
            batch("r2", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v2))),
            // 同一要求の再提案（リーダー変更後の再送に相当）
            batch("r1", listOf(KvCondition.Absent(k1)), listOf(KvMutation.Put(k1, v1))),
            batch("r3", listOf(KvCondition.RevisionEquals(k1, 1L)), listOf(KvMutation.Put(k1, w))),
        )
        val machine = newMachine()
        log.forEachIndexed { offset, command -> machine.applyAt(command, offset + 1L) }
        val afterFirstPass = machine.snapshotData()

        // 同じログを同じ状態機械へ再適用しても状態は変わらない
        log.forEachIndexed { offset, command -> machine.applyAt(command, offset + 1L) }
        assertContentEquals(afterFirstPass, machine.snapshotData())

        // 空の状態機械からのリプレイは同一の結果になる（決定性）
        val replayed = newMachine()
        log.forEachIndexed { offset, command -> replayed.applyAt(command, offset + 1L) }
        assertContentEquals(afterFirstPass, replayed.snapshotData())
        assertEquals(BatchOutcome.Applied(1L), receipt(replayed, "r1").outcome)
        assertEquals(BatchOutcome.Rejected(0, observedRevision = 1L), receipt(replayed, "r2").outcome)
        assertEquals(BatchOutcome.Applied(4L), receipt(replayed, "r3").outcome)
        assertContentEquals(w, replayed.get(k1))
        assertEquals(3, replayed.receipts.size)
    }

    @Test
    fun conditionalBatchCannotBeAppliedWithoutVersion() = runTest {
        val machine = newMachine()

        assertFailsWith<IllegalStateException> {
            machine.apply(batch("r1", emptyList(), listOf(KvMutation.Put(k1, v1))))
        }
        assertNull(machine.get(k1))
    }

    private fun newMachine() = StorageEngineStateMachine(MemoryMvccStorageEngine())

    private fun receipt(
        machine: StorageEngineStateMachine,
        requestId: String,
        principal: String = "tenant-a/alice",
    ): RequestReceipt = assertNotNull(machine.receipts.find(principal, requestId), "missing receipt $requestId")

    private fun batch(
        requestId: String,
        conditions: List<KvCondition> = emptyList(),
        mutations: List<KvMutation>,
        principal: String = "tenant-a/alice",
    ): RaftCommand.ConditionalBatch = RaftCommand.ConditionalBatch(
        ConditionalBatchRequest(principal, requestId, conditions, mutations),
    )
}
