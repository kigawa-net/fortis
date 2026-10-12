package net.kigawa.fortis.raft.txn

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.storage.engine.mvcc.MemoryMvccStorageEngine
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TxnCoordinatorTest {
    @Test
    fun singleGroupCommitSucceedsWithSnapshotConsistency() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val coordinator = TxnCoordinator(mapOf("g1" to TxnParticipant("g1", engine)))

        val txn = coordinator.begin()
        // スナップショット読みで旧値が見える
        assertContentEquals(byteArrayOf(10), txn.get("g1", byteArrayOf(1)))
        txn.put("g1", byteArrayOf(1), byteArrayOf(20))

        assertTrue(txn.commit())
        assertEquals(TxnState.COMMITTED, txn.record.state)
        // 確定後は新値、旧版は残る
        assertContentEquals(byteArrayOf(20), engine.get(byteArrayOf(1)))
        assertContentEquals(byteArrayOf(10), engine.getAt(byteArrayOf(1), 1))
    }

    @Test
    fun writeWriteConflictAborts() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val coordinator = TxnCoordinator(mapOf("g1" to TxnParticipant("g1", engine)))

        val txn = coordinator.begin()
        assertContentEquals(byteArrayOf(10), txn.get("g1", byteArrayOf(1)))
        // 他者が先に同キーを更新する
        engine.putAt(byteArrayOf(1), byteArrayOf(30), 2)
        txn.put("g1", byteArrayOf(1), byteArrayOf(20))

        assertFalse(txn.commit())
        assertEquals(TxnState.ABORTED, txn.record.state)
        // 他者の書き込みだけが残る
        assertContentEquals(byteArrayOf(30), engine.get(byteArrayOf(1)))
    }

    @Test
    fun crossShardCommitAppliesToBothGroups() = runTest {
        val engineA = MemoryMvccStorageEngine()
        val engineB = MemoryMvccStorageEngine()
        val coordinator = TxnCoordinator(
            mapOf(
                "g1" to TxnParticipant("g1", engineA),
                "g2" to TxnParticipant("g2", engineB),
            ),
        )

        val txn = coordinator.begin()
        txn.put("g1", byteArrayOf(1), byteArrayOf(11))
        txn.put("g2", byteArrayOf(2), byteArrayOf(22))

        assertTrue(txn.commit())
        assertContentEquals(byteArrayOf(11), engineA.get(byteArrayOf(1)))
        assertContentEquals(byteArrayOf(22), engineB.get(byteArrayOf(2)))
    }

    @Test
    fun prepareFailureAbortsWholeTransaction() = runTest {
        val engineA = MemoryMvccStorageEngine()
        val engineB = MemoryMvccStorageEngine()
        engineB.putAt(byteArrayOf(2), byteArrayOf(50), 1)
        val coordinator = TxnCoordinator(
            mapOf(
                "g1" to TxnParticipant("g1", engineA),
                "g2" to TxnParticipant("g2", engineB),
            ),
        )

        val txn = coordinator.begin()
        txn.put("g1", byteArrayOf(1), byteArrayOf(11))
        txn.put("g2", byteArrayOf(2), byteArrayOf(22))
        // g2 への競合書き込みで prepare が失敗する
        engineB.putAt(byteArrayOf(2), byteArrayOf(99), 2)

        assertFalse(txn.commit())
        assertEquals(TxnState.ABORTED, txn.record.state)
        // どちらのグループにも反映されない
        assertNull(engineA.get(byteArrayOf(1)))
        assertContentEquals(byteArrayOf(99), engineB.get(byteArrayOf(2)))
    }

    @Test
    fun recoverAbortsPreparedRemainder() = runTest {        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val participant = TxnParticipant("g1", engine)
        val coordinator = TxnCoordinator(mapOf("g1" to participant))

        // コーディネーター障害を模す: prepare 済みだが commit 未実行のまま残す
        val txn = coordinator.begin()
        txn.put("g1", byteArrayOf(1), byteArrayOf(20))
        assertTrue(participant.prepare(txn.record))
        txn.record.state = TxnState.PREPARED

        assertEquals(1, coordinator.recover())
        assertEquals(TxnState.ABORTED, txn.record.state)
        assertEquals(0, participant.stagedCount())
        // ステージングは破棄され、確定値は残らない
        assertContentEquals(byteArrayOf(10), engine.get(byteArrayOf(1)))

        // 二度目の回復は何もしない
        assertEquals(0, coordinator.recover())
    }

    @Test
    fun overlappingTransactionsConflictOnSameKey() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val coordinator = TxnCoordinator(mapOf("g1" to TxnParticipant("g1", engine)))

        // 同一スナップショットで開始した2件が同キーを書く
        val txn1 = coordinator.begin()
        val txn2 = coordinator.begin()
        txn1.put("g1", byteArrayOf(1), byteArrayOf(20))
        txn2.put("g1", byteArrayOf(1), byteArrayOf(30))

        assertTrue(txn1.commit())
        // 先行確定で版が進んだため後発は競合する
        assertFalse(txn2.commit())
        assertEquals(TxnState.ABORTED, txn2.record.state)
        assertContentEquals(byteArrayOf(20), engine.get(byteArrayOf(1)))
    }

    @Test
    fun preparedIntentBlocksOverlappingPrepare() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val participant = TxnParticipant("g1", engine)
        val coordinator = TxnCoordinator(mapOf("g1" to participant))

        val txn1 = coordinator.begin()
        txn1.put("g1", byteArrayOf(1), byteArrayOf(20))
        assertTrue(participant.prepare(txn1.record))

        // 版は進んでいないが write-intent が保持されているため拒否される
        val txn2 = coordinator.begin()
        txn2.put("g1", byteArrayOf(1), byteArrayOf(30))
        assertFalse(participant.prepare(txn2.record))

        // 破棄後は予約が解かれる
        participant.abort(txn1.record)
        txn1.record.state = TxnState.ABORTED
        assertTrue(participant.prepare(txn2.record))
        participant.abort(txn2.record)
        assertContentEquals(byteArrayOf(10), engine.get(byteArrayOf(1)))
    }

    @Test
    fun abaWriteIsDetectedByVersion() = runTest {
        val engine = MemoryMvccStorageEngine()
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 1)
        val coordinator = TxnCoordinator(mapOf("g1" to TxnParticipant("g1", engine)))

        val txn = coordinator.begin()
        assertContentEquals(byteArrayOf(10), txn.get("g1", byteArrayOf(1)))
        // 外部書き込みで値が変わって元に戻る（値は同じだが版は進む）
        engine.putAt(byteArrayOf(1), byteArrayOf(99), 2)
        engine.putAt(byteArrayOf(1), byteArrayOf(10), 3)
        txn.put("g1", byteArrayOf(1), byteArrayOf(20))

        // 値比較では検出できないが版比較で競合する
        assertFalse(txn.commit())
        assertEquals(TxnState.ABORTED, txn.record.state)
    }
}
