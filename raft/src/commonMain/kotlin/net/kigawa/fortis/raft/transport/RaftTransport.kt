package net.kigawa.fortis.raft.transport

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

/** Each result must belong to that call's request; responses must never be cached or reused. */
interface RaftTransport {
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
     * として扱い、複製を打ち切ること。チャンク分割転送は将来課題。
     */
    suspend fun installSnapshot(
        peerId: String,
        request: InstallSnapshotRequest,
    ): InstallSnapshotResponse {
        throw RaftTransportException("InstallSnapshot is not supported for peer: $peerId")
    }
}
