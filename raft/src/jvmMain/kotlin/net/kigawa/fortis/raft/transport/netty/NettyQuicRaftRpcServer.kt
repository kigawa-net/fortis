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
import io.netty.handler.codec.quic.QuicSslContext
import io.netty.handler.codec.quic.QuicStreamChannel
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.transport.RaftPeerAddress
import net.kigawa.fortis.raft.transport.RaftRpcHandler
import net.kigawa.fortis.raft.transport.RaftRpcServer
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult

class NettyQuicRaftRpcServer(
    private val address: RaftPeerAddress,
    private val handler: RaftRpcHandler,
    private val sslContext: QuicSslContext,
    private val codec: RaftRpcCodec = RaftRpcCodec(),
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
    private val idleTimeoutMillis: Long = DEFAULT_IDLE_TIMEOUT_MILLIS,
) : RaftRpcServer {
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
            val message = (codec.decode(frame) as RaftRpcDecodeResult.Success).message
            scope.launch {
                try {
                    val response = codec.encode(handler.handle(message))
                    context.writeAndFlush(Unpooled.wrappedBuffer(response))
                        .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT)
                } catch (cause: Throwable) {
                    context.fireExceptionCaught(cause)
                }
            }
        }

        override fun onFailure(cause: Throwable) = Unit
    }

    private fun maximumFrameSize(): Long =
        codec.headerSize.toLong() + codec.maxPayloadLength.toLong()

    companion object {
        private const val DEFAULT_IDLE_TIMEOUT_MILLIS = 5_000L
        private const val MAX_CONNECTION_DATA = 64L * 1024L * 1024L
        private const val MAX_CONCURRENT_STREAMS = 100L
    }
}
