package net.kigawa.fortis.raft.transport

import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.runtime.RaftRuntime
import net.kigawa.fortis.raft.snapshot.InstallSnapshotRequest
import net.kigawa.fortis.raft.snapshot.InstallSnapshotResponse
import net.kigawa.fortis.raft.snapshot.RaftSnapshotApplier
import net.kigawa.fortis.raft.snapshot.SnapshotMetadata
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.RaftRpcMessage
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

// InstallSnapshot の RPC 符号化・転送・受付配線を確認する
class InstallSnapshotRpcTest {
    private val codec = RaftRpcCodec()

    @Test
    fun installSnapshotRequestRoundTrips() {
        val message = RaftRpcMessage.InstallSnapshot(
            InstallSnapshotRequest(
                term = 3,
                leaderId = "指導者-1",
                metadata = SnapshotMetadata(10, 2),
                data = byteArrayOf(1, 2, 3, 250.toByte()),
            ),
        )

        val decoded = codec.decode(codec.encode(message))

        assertIs<RaftRpcDecodeResult.Success>(decoded)
        assertEquals(message, decoded.message)
        assertEquals(codec.encode(message).size, decoded.bytesRead)
    }

    @Test
    fun installSnapshotResponseRoundTrips() {
        val message = RaftRpcMessage.InstallSnapshotResult(InstallSnapshotResponse(term = 4))

        val decoded = codec.decode(codec.encode(message))

        assertIs<RaftRpcDecodeResult.Success>(decoded)
        assertEquals(message, decoded.message)
    }

    @Test
    fun truncatedInstallSnapshotIsCorrupted() {
        val frame = codec.encode(
            RaftRpcMessage.InstallSnapshot(
                InstallSnapshotRequest(
                    term = 1,
                    leaderId = "leader",
                    metadata = SnapshotMetadata(2, 1),
                    data = byteArrayOf(9),
                ),
            ),
        )

        val decoded = codec.decode(frame.copyOf(frame.size - 1))

        assertIs<RaftRpcDecodeResult>(decoded)
        assertTrue(decoded is RaftRpcDecodeResult.Corrupted || decoded is RaftRpcDecodeResult.Incomplete)
    }

    @Test
    fun rpcTransportSendsInstallSnapshot() = runTest {
        val request = InstallSnapshotRequest(
            term = 2,
            leaderId = "leader",
            metadata = SnapshotMetadata(5, 2),
            data = byteArrayOf(7, 8),
        )
        val response = InstallSnapshotResponse(term = 2)
        val channel = RespondingChannel(
            codec.encode(RaftRpcMessage.InstallSnapshotResult(response)),
        )

        assertEquals(response, RpcRaftTransport(channel, codec).installSnapshot("peer", request))
        assertEquals(
            RaftRpcMessage.InstallSnapshot(request),
            (codec.decode(channel.requestFrame) as RaftRpcDecodeResult.Success).message,
        )
    }

    @Test
    fun installSnapshotTravelsThroughRpcHandler() = runTest {
        val channel = InMemoryRaftRpcChannel(codec)
        val transport = RpcRaftTransport(channel, codec)
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val applied = mutableListOf<Pair<SnapshotMetadata, ByteArray>>()
        val follower = RaftNodeBuilder(
            nodeId = "node-2",
            peerIds = setOf("node-1"),
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = RecordingStateMachine(),
        ).build()
        val runtime = RaftRuntime(
            follower,
            transport,
            snapshotApplier = RaftSnapshotApplier { metadata, data ->
                applied.add(metadata to data.copyOf())
            },
        )
        channel.register("node-2", RaftRpcHandler(runtime))

        val response = transport.installSnapshot(
            "node-2",
            InstallSnapshotRequest(
                term = 1,
                leaderId = "node-1",
                metadata = SnapshotMetadata(3, 1),
                data = byteArrayOf(4, 5),
            ),
        )

        assertEquals(InstallSnapshotResponse(term = 1), response)
        assertEquals(3, volatileState.commitIndex)
        assertEquals(3, volatileState.lastApplied)
        assertEquals(1, applied.size)
        assertTrue(applied.single().second.contentEquals(byteArrayOf(4, 5)))
    }

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }

    private class RespondingChannel(
        private val responseFrame: ByteArray,
    ) : RaftRpcChannel {
        lateinit var requestFrame: ByteArray

        override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
            requestFrame = frame
            return responseFrame
        }
    }
}
