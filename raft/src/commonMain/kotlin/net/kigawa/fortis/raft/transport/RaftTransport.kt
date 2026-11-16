package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunk
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunkResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

/** Each result must belong to that call's request; responses must never be cached or reused. */
interface RaftTransport {
    /** チャンクを送信する。既存実装は未対応として扱う。 */
    suspend fun installSnapshotChunk(peerId: String, chunk: InstallSnapshotChunk): InstallSnapshotChunkResponse {
        throw RaftTransportException("Chunked InstallSnapshot is not supported for peer: $peerId")
    }

    suspend fun requestVote(
        peerId: String,
        request: RequestVoteRequest,
    ): RequestVoteResponse

    suspend fun appendEntries(
        peerId: String,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse

    /**
     * 遅れたフォロワーへ単一メッセージのスナップショットを送る。
     *
     * 既定では未対応として [RaftTransportException] を投げるため、
     * 既存実装を壊さない。呼び出し側は未対応時も [RaftTransportException]
     * として扱い、複製を打ち切ること。分割転送は [installSnapshotChunk] を使う。
     */
    suspend fun installSnapshot(
        peerId: String,
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse {
        throw RaftTransportException("InstallSnapshot is not supported for peer: $peerId")
    }
}
