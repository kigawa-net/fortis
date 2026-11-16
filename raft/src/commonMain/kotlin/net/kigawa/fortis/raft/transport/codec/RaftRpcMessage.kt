package net.kigawa.fortis.raft.transport.codec

import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.append.AppendEntriesResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunk
import net.kigawa.fortis.raft.snapshot.InstallSnapshotChunkResponse
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse

sealed interface RaftRpcMessage {
    /** 追加の分割転送要求。 */
    data class SnapshotChunk(val chunk: InstallSnapshotChunk) : RaftRpcMessage

    /** 分割転送の受信確認。 */
    data class SnapshotChunkResult(val response: InstallSnapshotChunkResponse) : RaftRpcMessage

    data class RequestVote(
        val request: RequestVoteRequest,
    ) : RaftRpcMessage

    data class RequestVoteResult(
        val response: RequestVoteResponse,
    ) : RaftRpcMessage

    data class AppendEntries(
        val request: AppendEntriesRequest,
    ) : RaftRpcMessage

    data class AppendEntriesResult(
        val response: AppendEntriesResponse,
    ) : RaftRpcMessage

    /** 単一メッセージの InstallSnapshot 要求。 */
    data class InstallSnapshot(
        val request: InstallSnapshotRequest,
    ) : RaftRpcMessage

    /** 単一メッセージの InstallSnapshot 応答。 */
    data class InstallSnapshotResult(
        val response: InstallSnapshotResponse,
    ) : RaftRpcMessage
}
