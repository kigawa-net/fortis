package net.kigawa.fortis.raft.runtime

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import net.kigawa.fortis.raft.MemoryRaftPersistentStateStore
import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.RaftStateMachine
import net.kigawa.fortis.raft.append.AppendEntriesRequest
import net.kigawa.fortis.raft.candidate.CandidateNode
import net.kigawa.fortis.raft.leader.LeaderNode
import net.kigawa.fortis.raft.log.MemoryRaftLog
import net.kigawa.fortis.raft.log.RaftLogEntry
import net.kigawa.fortis.raft.node.RaftNodeBuilder
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.RaftTransport
import net.kigawa.fortis.raft.transport.RpcRaftTransport
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcChannel
import net.kigawa.fortis.raft.transport.netty.NettyQuicRaftRpcServer
import net.kigawa.fortis.raft.transport.netty.NettyRaftTlsConfig
import net.kigawa.fortis.raft.transport.netty.TestRaftCertificateAuthority
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteRequest
import net.kigawa.fortis.raft.vote.RequestVoteResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class NettyQuicRaftRpcIntegrationTest {
    @Test
    fun simultaneousElectionsReceiveVoteResponsesOverQuicWithoutMutualWaiting() = runNetworkTest {
        val authority = TestRaftCertificateAuthority.create()
        val nodeIds = setOf("node-1", "node-2")
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val tlsConfigs = nodeIds.associateWith { tlsConfig(authority, it) }
        val channels = nodeIds.associateWith {
            NettyQuicRaftRpcChannel(resolver(addresses), tlsConfigs.getValue(it))
        }
        val bothSending = CompletableDeferred<Unit>()
        val started = AtomicInteger()
        val completed = AtomicInteger()
        val nodes = nodeIds.associateWith { nodeId ->
            val rpc = RpcRaftTransport(channels.getValue(nodeId))
            val transport = object : RaftTransport by rpc {
                override suspend fun requestVote(peerId: String, request: RequestVoteRequest): RequestVoteResponse {
                    if (started.incrementAndGet() == 2) bothSending.complete(Unit)
                    bothSending.await()
                    return rpc.requestVote(peerId, request).also { completed.incrementAndGet() }
                }
            }
            node(nodeId, transport, peerIds = nodeIds - nodeId)
        }
        val servers = nodeIds.associateWith { server(nodes.getValue(it), tlsConfigs.getValue(it)) }
        try {
            startServers(servers, addresses)
            withTimeout(5.seconds) {
                nodes.values.map { async { it.runtime.onElectionTimeout() } }.awaitAll()
            }
            assertEquals(2, completed.get())
            for (fixture in nodes.values) {
                assertIs<CandidateNode>(fixture.runtime.currentNode)
                assertEquals(1L, fixture.runtime.currentNode.persistentState.currentTerm)
            }
        } finally {
            close(channels.values, servers.values)
        }
    }

    @Test
    fun connectTimeoutAllowsLaterReconnectToAvailablePeer() = runNetworkTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses), tlsConfig(authority, "node-1"), connectTimeout = 500.milliseconds,
        )
        val transport = RpcRaftTransport(channel)
        val server = server(node("node-2", transport), tlsConfig(authority, "node-2"))
        try {
            // A bound UDP socket silently drops QUIC packets instead of returning ICMP errors.
            val blackhole = withContext(Dispatchers.IO) {
                DatagramSocket(InetSocketAddress("127.0.0.1", 0))
            }
            blackhole.use {
                addresses["node-2"] = RaftPeerAddress("127.0.0.1", it.localPort)
                val error = withTimeout(3.seconds) {
                    assertFailsWith<RaftTransportException> {
                        transport.requestVote("node-2", voteRequest("node-1"))
                    }
                }
                assertEquals("Raft RPC connection to node-2 timed out after 500ms", error.message)
                assertEquals(0, channel.connectionCreationCount)
            }
            server.start()
            addresses["node-2"] = server.boundAddress
            assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
            assertEquals(1, channel.connectionCreationCount)
        } finally {
            close(listOf(channel), listOf(server))
        }
    }

    @Test
    fun requestTimeoutEvictsConnectionAndAllowsNextRpcToReconnect() = runNetworkTest {
        stalledRequest(callerCancels = false)
    }

    @Test
    fun callerTimeoutRemainsCancellationAndAllowsNextRpcOnSameConnection() = runNetworkTest {
        stalledRequest(callerCancels = true)
    }

    private suspend fun stalledRequest(callerCancels: Boolean) {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val stateMachine = RecordingStateMachine {
            entered.complete(Unit)
            release.await()
        }
        val channel = NettyQuicRaftRpcChannel(
            resolver(addresses), tlsConfig(authority, "node-1"),
            requestTimeout = if (callerCancels) 5.seconds else 500.milliseconds,
        )
        val transport = RpcRaftTransport(channel)
        val fixture = node("node-2", transport, stateMachine)
        val server = server(fixture, tlsConfig(authority, "node-2"))
        val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))
        val request = AppendEntriesRequest(
            term = 1, leaderId = "node-1", prevLogIndex = 0, prevLogTerm = 0,
            entries = listOf(RaftLogEntry(index = 1, term = 1, command = command)),
            leaderCommit = 1,
        )
        try {
            server.start()
            addresses["node-2"] = server.boundAddress
            // Complete the handshake before timing only the stalled RPC.
            assertTrue(transport.requestVote("node-2", voteRequest("node-1")).voteGranted)
            if (callerCancels) {
                assertFailsWith<TimeoutCancellationException> {
                    withTimeout(500.milliseconds) { transport.appendEntries("node-2", request) }
                }
            } else {
                val error = withTimeout(3.seconds) {
                    assertFailsWith<RaftTransportException> {
                        transport.appendEntries("node-2", request)
                    }
                }
                assertEquals("Raft RPC request to node-2 timed out after 500ms", error.message)
            }
            assertTrue(entered.isCompleted)
            assertEquals(emptyList(), stateMachine.applied)
            release.complete(Unit)
            assertTrue(transport.appendEntries("node-2", request).success)
            assertEquals(listOf<RaftCommand>(command), stateMachine.applied)
            assertEquals(1L, fixture.volatileState.lastApplied)
            assertEquals(if (callerCancels) 1 else 2, channel.connectionCreationCount)
        } finally {
            release.complete(Unit)
            close(listOf(channel), listOf(server))
        }
    }

    @Test
    fun timedOutFollowerDoesNotPreventHeartbeatToHealthyFollower() = runNetworkTest {
        val authority = TestRaftCertificateAuthority.create()
        val addresses = mutableMapOf<String, RaftPeerAddress>()
        val release = CompletableDeferred<Unit>()
        val entered = CompletableDeferred<Unit>()
        val healthyHeartbeats = AtomicInteger()
        val stalledStateMachine = RecordingStateMachine {
            entered.complete(Unit)
            release.await()
        }
        val tlsConfigs = NODE_IDS.associateWith { tlsConfig(authority, it) }
        val channels = NODE_IDS.associateWith {
            NettyQuicRaftRpcChannel(
                resolver(addresses), tlsConfigs.getValue(it), requestTimeout = 500.milliseconds,
            )
        }
        val nodes = NODE_IDS.associateWith {
            node(
                it, RpcRaftTransport(channels.getValue(it)),
                if (it == "node-2") stalledStateMachine else RecordingStateMachine(),
                if (it == "node-3") RaftTimer { healthyHeartbeats.incrementAndGet() } else RaftTimer.None,
            )
        }
        val servers = NODE_IDS.associateWith { server(nodes.getValue(it), tlsConfigs.getValue(it)) }
        try {
            startServers(servers, addresses)
            val leader = nodes.getValue("node-1")
            leader.runtime.onElectionTimeout()
            assertIs<LeaderNode>(leader.runtime.currentNode)
            val command = RaftCommand.Put(byteArrayOf(1), byteArrayOf(10))
            leader.runtime.appendCommand(command)
            val heartbeatsBeforeTimeout = healthyHeartbeats.get()
            withTimeout(3.seconds) { leader.runtime.onHeartbeatTimeout() }
            assertEquals(heartbeatsBeforeTimeout + 1, healthyHeartbeats.get())
            assertTrue(entered.isCompleted)
            assertIs<LeaderNode>(leader.runtime.currentNode)
            assertEquals(1L, leader.volatileState.commitIndex)
            assertEquals(listOf<RaftCommand>(command), leader.stateMachine.applied)
            assertEquals(listOf<RaftCommand>(command), nodes.getValue("node-3").stateMachine.applied)
            assertEquals(emptyList(), stalledStateMachine.applied)
            release.complete(Unit)
            leader.runtime.onHeartbeatTimeout()
            assertEquals(listOf<RaftCommand>(command), stalledStateMachine.applied)
            assertEquals(3, channels.getValue("node-1").connectionCreationCount)
        } finally {
            release.complete(Unit)
            close(channels.values, servers.values)
        }
    }

    @Test
    fun reconnectsAfterServerRestartsOnSamePort() = runNetworkTest {
        reconnectAfterServerRestart(changePort = false)
    }

    @Test
    fun reconnectsToUpdatedResolverAddressAfterServerRestarts() = runNetworkTest {
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
    fun offlineFollowerCatchesUpAndAppliesCommittedCommandsAfterQuicServerRestart() = runNetworkTest {
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
    fun threeNodesElectReplicateCommitAndApplyOverMutualTlsQuic() = runNetworkTest {
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
    fun serverRejectsClientCertificateFromUnknownAuthority() = runNetworkTest {
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
    fun clientRejectsCertificateForWrongPeerId() = runNetworkTest {
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
    fun serverRejectsCertificateForUnknownPeer() = runNetworkTest {
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
    fun serverRejectsRpcClaimingAnotherPeerId() = runNetworkTest {
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
        transport: RaftTransport,
        stateMachine: RecordingStateMachine = RecordingStateMachine(),
        timer: RaftTimer = RaftTimer.None,
        peerIds: Set<String> = NODE_IDS - nodeId,
    ): NodeFixture {
        val volatileState = RaftVolatileState()
        val log = MemoryRaftLog()
        val initialNode = RaftNodeBuilder(
            nodeId = nodeId,
            peerIds = peerIds,
            persistentStateStore = MemoryRaftPersistentStateStore(),
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
            timer = timer,
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

    private class RecordingStateMachine(
        private val beforeApply: suspend () -> Unit = {},
    ) : RaftStateMachine {
        val applied = mutableListOf<RaftCommand>()

        override suspend fun apply(command: RaftCommand) {
            beforeApply()
            applied.add(command)
        }
    }

    // Netty uses wall-clock deadlines; run coroutine timeouts on the same clock.
    private fun runNetworkTest(block: suspend CoroutineScope.() -> Unit) = runTest {
        withContext(Dispatchers.Default, block)
    }

    private companion object {
        val NODE_IDS = setOf("node-1", "node-2", "node-3")
    }
}
