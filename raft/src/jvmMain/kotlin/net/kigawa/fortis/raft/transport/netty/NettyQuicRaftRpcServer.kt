package net.kigawa.fortis.raft.transport.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.quic.QuicServerCodecBuilder
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicStreamChannel
import java.net.InetSocketAddress
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.sharding.RaftGroupManager
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RaftRpcServer
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult
import net.kigawa.fortis.raft.transport.codec.senderId

class NettyQuicRaftRpcServer private constructor(
    private val address: RaftPeerAddress,
    tlsConfig: NettyRaftTlsConfig,
    private val codec: RaftRpcCodec,
    dispatcher: CoroutineDispatcher,
    private val idleTimeoutMillis: Long,
    private val handleFrame: suspend (String, ByteArray) -> ByteArray,
) : RaftRpcServer {
    constructor(
        address: RaftPeerAddress,
        handler: RaftRpcHandler,
        tlsConfig: NettyRaftTlsConfig,
        codec: RaftRpcCodec = RaftRpcCodec(),
        dispatcher: CoroutineDispatcher = Dispatchers.Default,
        idleTimeoutMillis: Long = DEFAULT_IDLE_TIMEOUT_MILLIS,
    ) : this(address, tlsConfig, codec, dispatcher, idleTimeoutMillis, { peerId, frame ->
        val decoded = codec.decode(frame) as? RaftRpcDecodeResult.Success
            ?: throw RaftTransportException("Expected a single-group Raft RPC")
        if (!handler.isKnownPeer(peerId)) {
            throw RaftTransportException("Authenticated Raft peer is not a cluster member: $peerId")
        }
        requireSender(peerId, decoded.message.senderId())
        codec.encode(handler.handle(decoded.message))
    })

    private val sslContext = NettyQuicSslContextFactory.server(tlsConfig)
    private val eventLoopGroup: EventLoopGroup =
        MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val mutex = Mutex()
    @Volatile
    private var channel: Channel? = null
    private var stopped = false

    val boundAddress: RaftPeerAddress
        get() {
            val socketAddress = channel?.localAddress() as? InetSocketAddress
                ?: error("Raft RPC server is not started")
            return RaftPeerAddress(
                socketAddress.address?.hostAddress ?: socketAddress.hostString,
                socketAddress.port,
            )
        }

    override suspend fun start() {
        mutex.withLock {
            check(!stopped) { "Raft RPC server has been stopped" }
            if (channel?.isActive == true) return

            val serverCodec = QuicServerCodecBuilder()
                .sslContext(sslContext)
                .maxIdleTimeout(idleTimeoutMillis, TimeUnit.MILLISECONDS)
                .initialMaxData(MAX_CONNECTION_DATA)
                .initialMaxStreamDataBidirectionalLocal(maximumFrameSize())
                .initialMaxStreamDataBidirectionalRemote(maximumFrameSize())
                .initialMaxStreamsBidirectional(MAX_CONCURRENT_STREAMS)
                .activeMigration(false)
                .handler(object : ChannelInboundHandlerAdapter() {
                    override fun isSharable(): Boolean = true
                })
                .streamHandler(object : ChannelInitializer<QuicStreamChannel>() {
                    override fun initChannel(stream: QuicStreamChannel) {
                        stream.pipeline().addLast(RequestHandler())
                    }
                })
                .build()

            channel = Bootstrap()
                .group(eventLoopGroup)
                .channel(NioDatagramChannel::class.java)
                .handler(serverCodec)
                .bind(InetSocketAddress(address.host, address.port))
                .let { future ->
                    future.awaitCompletion()
                    future.channel()
                }
        }
    }

    override suspend fun stop() {
        val currentChannel = mutex.withLock {
            if (stopped) return
            stopped = true
            channel.also { channel = null }
        }
        currentChannel?.close()?.awaitCompletion()
        scope.cancel()
        eventLoopGroup.shutdownGracefully().awaitCompletion()
    }

    private inner class RequestHandler : NettyRaftRpcFrameHandler(codec) {
        override fun onFrame(context: ChannelHandlerContext, frame: ByteArray) {
            val authenticatedPeerId = authenticatedPeerId(context)
            scope.launch {
                try {
                    val response = handleFrame(authenticatedPeerId, frame)
                    context.writeAndFlush(Unpooled.wrappedBuffer(response))
                        .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT)
                } catch (cause: Throwable) {
                    context.close()
                }
            }
        }

        override fun onFailure(cause: Throwable) = Unit
    }

    private fun authenticatedPeerId(context: ChannelHandlerContext): String {
        val connection = context.channel().parent() as? QuicChannel
            ?: throw RaftTransportException("Raft RPC stream has no QUIC connection")
        val certificate = connection.sslEngine()
            ?.session
            ?.peerCertificates
            ?.firstOrNull() as? X509Certificate
            ?: throw RaftTransportException(
                "Raft TLS client did not provide an X.509 certificate",
            )
        return NettyRaftCertificateIdentity.peerId(certificate)
    }

    private fun maximumFrameSize(): Long =
        codec.headerSize.toLong() + codec.maxPayloadLength.toLong()

    companion object {
        /** 共有 QUIC サーバーでグループ付き RPC を受信する。 */
        fun forGroups(
            address: RaftPeerAddress,
            manager: RaftGroupManager,
            tlsConfig: NettyRaftTlsConfig,
            codec: RaftRpcCodec = RaftRpcCodec(),
            dispatcher: CoroutineDispatcher = Dispatchers.Default,
            idleTimeoutMillis: Long = DEFAULT_IDLE_TIMEOUT_MILLIS,
        ): NettyQuicRaftRpcServer =
            NettyQuicRaftRpcServer(address, tlsConfig, codec, dispatcher, idleTimeoutMillis) { peerId, frame ->
                val request = codec.decodeGroup(frame)
                requireSender(peerId, request.message.senderId())
                codec.encodeGroup(manager.handleRpc(request))
            }

        private fun requireSender(authenticated: String, claimed: String) {
            if (authenticated != claimed) {
                throw RaftTransportException("Authenticated Raft peer $authenticated cannot send RPC as $claimed")
            }
        }

        private const val DEFAULT_IDLE_TIMEOUT_MILLIS = 5_000L
        private const val MAX_CONNECTION_DATA = 64L * 1024L * 1024L
        private const val MAX_CONCURRENT_STREAMS = 100L
    }
}
