@file:Suppress("DEPRECATION")

package net.kigawa.fortis.raft.runtime

import io.netty.handler.codec.quic.QuicSslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.handler.ssl.util.SelfSignedCertificate
import kotlinx.coroutines.test.runTest
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcChannel
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcServer
import net.kigawa.fortis.raft.vote.RaftVolatileState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class NettyQuicRaftRpcIntegrationTest {
    @Test
    fun threeNodesElectReplicateCommitAndApplyOverLocalhostQuic() = runTest {
        val certificate = SelfSignedCertificate()
        val serverSslContext = QuicSslContextBuilder.forServer(
            certificate.privateKey(),
            null,
            certificate.certificate(),
        ).applicationProtocols(NettyQuicRaftRpcChannel.ALPN).build()
        val clientSslContext = QuicSslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .applicationProtocols(NettyQuicRaftRpcChannel.ALPN)
            .build()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val resolver = RaftPeerResolver { peerId -> addresses.getValue(peerId) }
        val channel = NettyQuicRaftRpcChannel(resolver, clientSslContext)
        val transport = RpcRaftTransport(channel)
        val nodes = NODE_IDS.associateWith { nodeId -> node(nodeId, transport) }
        val servers = nodes.map { (nodeId, fixture) ->
            nodeId to NettyQuicRaftRpcServer(
                RaftPeerAddress("127.0.0.1", 0),
                RaftRpcHandler(fixture.runtime),
                serverSslContext,
            )
        }

        try {
            for ((nodeId, server) in servers) {
                server.start()
                addresses[nodeId] = server.boundAddress
            }
            val leader = nodes.getValue("node-1")

            leader.runtime.onElectionTimeout()
            assertIs<LeaderNode>(leader.runtime.currentNode)

            val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))
            leader.runtime.appendCommand(command)
            leader.runtime.onHeartbeatTimeout()

            for (fixture in nodes.values) {
                assertEquals(command, fixture.log.get(1)?.command)
                assertEquals(1L, fixture.volatileState.commitIndex)
                assertEquals(1L, fixture.volatileState.lastApplied)
                assertEquals(listOf<RaftCommand>(command), fixture.stateMachine.applied)
            }
            assertEquals(2, channel.connectionCreationCount)
        } finally {
            try {
                channel.close()
            } finally {
                try {
                    servers.forEach { (_, server) -> server.stop() }
                } finally {
                    certificate.delete()
                }
            }
        }
    }

    private suspend fun node(
        nodeId: String,
        transport: RpcRaftTransport,
    ): NodeFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val stateMachine = RecordingStateMachine()
        val initialNode = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = NODE_IDS - nodeId,
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ).build()
        return NodeFixture(
            RaftRuntime(initialNode, transport),
            volatileState,
            log,
            stateMachine,
        )
    }

    private data class NodeFixture(
        val runtime: RaftRuntime,
        val volatileState: RaftVolatileState,
        val log: MemoryRaftLog,
        val stateMachine: RecordingStateMachine,
    )

    private class RecordingStateMachine : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            applied.add(command)
        }
    }

    private companion object {
        val NODE_IDS = setOf("node-1", "node-2", "node-3")
    }
}
