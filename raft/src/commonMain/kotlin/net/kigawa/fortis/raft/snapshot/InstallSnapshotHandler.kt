package net.kigawa.fortis.raft.snapshot

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.RaftPersistentState
import net.kigawa.fortis.raft.RaftPersistentStateStore
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.node.RaftTimeoutEvent
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.vote.RaftVolatileState

/** スナップショットを保存・適用してからログを圧縮する。 */
class InstallSnapshotHandler(
    private val persistentState: RaftPersistentState,
    private val persistentStateStore: RaftPersistentStateStore,
    private val volatileState: RaftVolatileState,
    private val log: RaftLog,
    private val timer: RaftTimer,
    private val snapshotApplier: RaftSnapshotApplier = RaftSnapshotApplier.NoOp,
    initialLastIncludedIndex: Long = 0,
    private val snapshotStore: FileRaftSnapshotStore? = null,
) {
    private val mutex = Mutex()
    private val reassembler = SnapshotReassembler()
    private var lastIncludedIndex = initialLastIncludedIndex
        .also { require(it >= 0) { "適用済みスナップショットのインデックスは負であってはならない" } }

    suspend fun handle(
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse = mutex.withLock {
        if (request.term < persistentState.currentTerm) {
            return@withLock InstallSnapshotResponse(persistentState.currentTerm)
        }

        if (request.term > persistentState.currentTerm) {
            persistentStateStore.save(request.term, null)
            persistentState.currentTerm = request.term
            persistentState.votedFor = null
        }

        timer.reset(RaftTimeoutEvent.Election)

        if (request.metadata.lastIncludedIndex <= maxOf(lastIncludedIndex, volatileState.commitIndex)) {
            return@withLock InstallSnapshotResponse(persistentState.currentTerm)
        }

        reassembler.discard()
        installSnapshot(request.metadata, request.data)
        InstallSnapshotResponse(persistentState.currentTerm)
    }

    /** チャンクを受信し、終端まで状態機械を変更しない。開始片で再送を開始できる。 */
    suspend fun handleChunk(chunk: InstallSnapshotChunk): InstallSnapshotChunkResponse = mutex.withLock {
        if (chunk.term < persistentState.currentTerm) {
            return@withLock InstallSnapshotChunkResponse(persistentState.currentTerm, false, 0)
        }
        if (chunk.term > persistentState.currentTerm) {
            persistentStateStore.save(chunk.term, null)
            persistentState.currentTerm = chunk.term
            persistentState.votedFor = null
            reassembler.discard()
        }
        timer.reset(RaftTimeoutEvent.Election)
        if (chunk.offset == 0L) reassembler.discard()
        val data = try {
            reassembler.accept(chunk)
        } catch (_: IllegalArgumentException) {
            reassembler.discard()
            return@withLock InstallSnapshotChunkResponse(persistentState.currentTerm, false, 0)
        }
        val nextOffset = reassembler.expectedOffset
        if (data != null) {
            reassembler.discard()
            if (chunk.metadata.lastIncludedIndex > maxOf(lastIncludedIndex, volatileState.commitIndex)) {
                installSnapshot(chunk.metadata, data)
            }
        }
        InstallSnapshotChunkResponse(persistentState.currentTerm, true, nextOffset)
    }

    private suspend fun installSnapshot(
        metadata: SnapshotMetadata,
        data: ByteArray,
    ) {
        snapshotStore?.save(metadata, data)
        snapshotApplier.apply(metadata, data)
        volatileState.commitIndex = maxOf(volatileState.commitIndex, metadata.lastIncludedIndex)
        volatileState.lastApplied = maxOf(volatileState.lastApplied, metadata.lastIncludedIndex)
        log.compactThrough(metadata, volatileState.lastApplied)
        lastIncludedIndex = metadata.lastIncludedIndex
    }
}
