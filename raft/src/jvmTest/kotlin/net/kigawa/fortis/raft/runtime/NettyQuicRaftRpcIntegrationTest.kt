package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.Dispatchers
import java.net.DatagramSocket
import java.net.InetSocketAddress
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcChannel
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcServer
import net.kigawa.fortis.raft.transport.netty.NettyRaftTlsConfig
import net.kigawa.fortis.raft.transport.netty.TestRaftCertificateAuthority
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class NettyQuicRaftRpcIntegrationTest {
    @Test
    fun reconnectsAfterServerRestartsOnSamePort() = runTest {
        reconnectAfterServerRestart(changePort = false)
    }

    @Test
    fun reconnectsToUpdatedResolverAddressAfterServerRestarts() = runTest {
        reconnectAfterServerRestart(changePort = true)
    }

    private suspend fun reconnectAfterServerRestart(changePort: Boolean) {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(resolver(addresses), tlsConfig(authority, "node-1"))
        val transport = RpcRaftTransport(channel)
        val fixture = node("node-2", transport)
        val serverTls = tlsConfig(authority, "node-2")
        val original = server(fixture, serverTls)
        val servers = mutableListOf(original)
        try {
            original.start()
            val oldAddress = original.boundAddress
            addresses["node-2"] = oldAddress
            assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
            assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
            assertEquals(1, channel.connectionCreationCount)

            original.stop()
            assertFailsWith<RaftTransportException> {
                transport.requestVote("node-2", voteRequest("node-1"))
            }
            assertEquals(1, channel.connectionCreationCount)

            // Reserve the old port so an ephemeral bind cannot accidentally reuse it.
            val reservation = if (changePort) {
                withContext(Dispatchers.IO) {
                    DatagramSocket(InetSocketAddress(oldAddress.host, oldAddress.port))
                }
            } else {
                null
            }
            reservation.use {
                val restarted = server(
                    fixture,
                    serverTls,
                    if (changePort) RaftPeerAddress("127.0.0.1", 0) else oldAddress,
                )
                servers.add(restarted)
                restarted.start()
                if (changePort) {
                    assertNotEquals(oldAddress.port, restarted.boundAddress.port)
                } else {
                    assertEquals(oldAddress.port, restarted.boundAddress.port)
                }
                addresses["node-2"] = restarted.boundAddress
                assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
                assertEquals(2, channel.connectionCreationCount)
                assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
                assertEquals(2, channel.connectionCreationCount)
            }
        } finally {
            close(listOf(channel), servers)
        }
    }

    @Test
    fun offlineFollowerCatchesUpAndAppliesCommittedCommandsAfterQuicServerRestart() = runTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val tlsConfigs = NODE_IDS.associateWith { tlsConfig(authority, it) }
        val channels = NODE_IDS.associateWith {
            NettyQuicRaftRpcChannel(resolver(addresses), tlsConfigs.getValue(it))
        }
        val nodes = NODE_IDS.associateWith { node(it, RpcRaftTransport(channels.getValue(it))) }
        val servers = NODE_IDS.associateWith { server(nodes.getValue(it), tlsConfigs.getValue(it)) }
        val allServers = servers.values.toMutableList()
        try {
            startServers(servers, addresses)
            val leader = nodes.getValue("node-1")
            val recovered = nodes.getValue("node-3")
            val leaderChannel = channels.getValue("node-1")
            leader.runtime.onElectionTimeout()
            assertIs<LeaderNode>(leader.runtime.currentNode)
            leader.runtime.onHeartbeatTimeout()
            assertEquals(2, leaderChannel.connectionCreationCount)

            val oldAddress = servers.getValue("node-3").boundAddress
            servers.getValue("node-3").stop()
            val commands: List<RaftCommand> = (1..3).map { value ->
                RaftCommand.Put(byteArrayOf(value.toByte()), byteArrayOf((value * 10).toByte()))
            }
            commands.forEach { leader.runtime.appendCommand(it) }
            leader.runtime.onHeartbeatTimeout()

            assertIs<LeaderNode>(leader.runtime.currentNode)
            for (nodeId in listOf("node-1", "node-2")) {
                val fixture = nodes.getValue(nodeId)
                assertEquals(commands, (1L..3L).map { fixture.log.get(it)?.command })
                assertEquals(3L, fixture.volatileState.commitIndex)
                assertEquals(3L, fixture.volatileState.lastApplied)
                assertEquals(commands, fixture.stateMachine.applied)
            }
            assertEquals(0L, recovered.log.lastIndex())
            assertEquals(0L, recovered.volatileState.commitIndex)
            assertEquals(0L, recovered.volatileState.lastApplied)
            assertEquals(emptyList(), recovered.stateMachine.applied)
            // Healthy-peer connections can also expire while the offline peer times out.
            val connectionsBeforeRecovery = leaderChannel.connectionCreationCount

            val restarted = server(recovered, tlsConfigs.getValue("node-3"), oldAddress)
            allServers.add(restarted)
            restarted.start()
            addresses["node-3"] = restarted.boundAddress
            leader.runtime.onHeartbeatTimeout()

            assertEquals(3L, recovered.log.lastIndex())
            assertEquals(commands, (1L..3L).map { recovered.log.get(it)?.command })
            assertEquals(3L, recovered.volatileState.commitIndex)
            assertEquals(3L, recovered.volatileState.lastApplied)
            assertEquals(commands, recovered.stateMachine.applied)
            assertTrue(leaderChannel.connectionCreationCount > connectionsBeforeRecovery)
            val connectionsAfterRecovery = leaderChannel.connectionCreationCount
            leader.runtime.onHeartbeatTimeout()
            assertEquals(commands, recovered.stateMachine.applied)
            assertEquals(connectionsAfterRecovery, leaderChannel.connectionCreationCount)
        } finally {
            close(channels.values, allServers)
        }
    }

    @Test
    fun threeNodesElectReplicateCommitAndApplyOverMutualTlsQuic() = runTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val resolver = resolver(addresses)
        val tlsConfigs = NODE_IDS.associateWith { nodeId -> tlsConfig(authority, nodeId) }
        val channels = NODE_IDS.associateWith { nodeId ->
            NettyQuicRaftRpcChannel(resolver, tlsConfigs.getValue(nodeId))
        }
        val nodes = NODE_IDS.associateWith { nodeId ->
            node(nodeId, RpcRaftTransport(channels.getValue(nodeId)))
        }
        val servers = NODE_IDS.associateWith { nodeId ->
            server(nodes.getValue(nodeId), tlsConfigs.getValue(nodeId))
        }

        try {
            startServers(servers, addresses)
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
            assertEquals(2, channels.getValue("node-1").connectionCreationCount)
        } finally {
            close(channels.values, servers.values)
        }
    }

    @Test
    fun serverRejectsClientCertificateFromUnknownAuthority() = runTest {
        val trustedAuthority = TestRaftCertificateAuthority.create()
        val unknownAuthority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses),
            NettyRaftTlsConfig(
                unknownAuthority.identity("node-1"),
                listOf(trustedAuthority.certificate),
            ),
        )
        val fixture = node("node-2", RpcRaftTransport(channel))
        val server = server(fixture, tlsConfig(trustedAuthority, "node-2"))

        try {
            server.start()
            addresses["node-2"] = server.boundAddress

            assertFailsWith<RaftTransportException> {
                RpcRaftTransport(channel).requestVote("node-2", voteRequest("node-1"))
            }
        } finally {
            close(listOf(channel), listOf(server))
        }
    }

    @Test
    fun clientRejectsCertificateForWrongPeerId() = runTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses),
            tlsConfig(authority, "node-1"),
        )
        val fixture = node("node-2", RpcRaftTransport(channel))
        val server = server(fixture, tlsConfig(authority, "node-3"))

        try {
            server.start()
            addresses["node-2"] = server.boundAddress

            val error = assertFailsWith<RaftTransportException> {
                RpcRaftTransport(channel).requestVote("node-2", voteRequest("node-1"))
            }
            assertEquals(
                "Raft TLS peer identity mismatch: expected node-2, got node-3",
                error.message,
            )
        } finally {
            close(listOf(channel), listOf(server))
        }
    }

    @Test
    fun serverRejectsCertificateForUnknownPeer() = runTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses),
            tlsConfig(authority, "node-4"),
        )
        val fixture = node("node-2", RpcRaftTransport(channel))
        val server = server(fixture, tlsConfig(authority, "node-2"))

        try {
            server.start()
            addresses["node-2"] = server.boundAddress

            assertFailsWith<RaftTransportException> {
                RpcRaftTransport(channel).requestVote("node-2", voteRequest("node-4"))
            }
        } finally {
            close(listOf(channel), listOf(server))
        }
    }

    @Test
    fun serverRejectsRpcClaimingAnotherPeerId() = runTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses),
            tlsConfig(authority, "node-1"),
        )
        val fixture = node("node-2", RpcRaftTransport(channel))
        val server = server(fixture, tlsConfig(authority, "node-2"))

        try {
            server.start()
            addresses["node-2"] = server.boundAddress

            assertFailsWith<RaftTransportException> {
                RpcRaftTransport(channel).requestVote("node-2", voteRequest("node-3"))
            }
        } finally {
            close(listOf(channel), listOf(server))
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

    private fun server(
        fixture: NodeFixture,
        tlsConfig: NettyRaftTlsConfig,
        address: RaftPeerAddress = RaftPeerAddress("127.0.0.1", 0),
    ) = NettyQuicRaftRpcServer(
        address,
        RaftRpcHandler(fixture.runtime),
        tlsConfig,
    )

    private suspend fun startServers(
        servers: Map<String, NettyQuicRaftRpcServer>,
        addresses: MutableMap<String, RaftPeerAddress>,
    ) {
        for ((nodeId, server) in servers) {
            server.start()
            addresses[nodeId] = server.boundAddress
        }
    }

    private suspend fun close(
        channels: Collection<NettyQuicRaftRpcChannel>,
        servers: Collection<NettyQuicRaftRpcServer>,
    ) {
        try {
            channels.forEach { it.close() }
        } finally {
            servers.forEach { it.stop() }
        }
    }

    private fun tlsConfig(
        authority: TestRaftCertificateAuthority,
        peerId: String,
    ) = NettyRaftTlsConfig(
        authority.identity(peerId),
        listOf(authority.certificate),
    )

    private fun resolver(addresses: Map<String, RaftPeerAddress>) =
        RaftPeerResolver { peerId -> addresses.getValue(peerId) }

    private fun voteRequest(candidateId: String) = RequestVoteRequest(
        term = 1,
        candidateId = candidateId,
        lastLogIndex = 0,
        lastLogTerm = 0,
    )

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
