package net.kigawa.fortis.raft.transport.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicClientCodecBuilder
import io.netty.handler.codec.quic.QuicSslContext
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.handler.codec.quic.QuicStreamType
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.raft.transport.RaftPeerResolver
import net.kigawa.fortis.raft.transport.RaftRpcChannel
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec

class NettyQuicRaftRpcChannel(
    private val peerResolver: RaftPeerResolver,
    private val sslContext: QuicSslContext,
    private val codec: RaftRpcCodec = RaftRpcCodec(),
    private val idleTimeoutMillis: Long = DEFAULT_IDLE_TIMEOUT_MILLIS,
) : RaftRpcChannel {
    private val eventLoopGroup: EventLoopGroup =
        MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    private val mutex = Mutex()
    private val connections = mutableMapOf<String, QuicChannel>()
    private var datagramChannel: Channel? = null
    private var closed = false
    internal var connectionCreationCount: Int = 0
        private set

    override suspend fun request(peerId: String, frame: ByteArray): ByteArray {
        val response = CompletableDeferred<ByteArray>()
        var connection: QuicChannel? = null
        var stream: QuicStreamChannel? = null
        try {
            connection = connection(peerId)
            stream = connection.createStream(
                QuicStreamType.BIDIRECTIONAL,
                ResponseHandler(codec, response),
            ).awaitResult()
            stream.writeAndFlush(Unpooled.wrappedBuffer(frame)).awaitCompletion()
            stream.shutdownOutput().awaitCompletion()
            return response.await()
        } catch (cause: Throwable) {
            connection?.takeIf { !it.isActive }?.let { evict(peerId, it) }
            if (cause is CancellationException) throw cause
            if (cause is RaftTransportException) throw cause
            throw RaftTransportException("Raft RPC request to $peerId failed", cause)
        } finally {
            stream?.close()
        }
    }

    suspend fun close() {
        val resources = mutex.withLock {
            if (closed) return
            closed = true
            val currentConnections = connections.values.toList()
            connections.clear()
            val currentDatagramChannel = datagramChannel
            datagramChannel = null
            currentConnections to currentDatagramChannel
        }
        resources.first.forEach { it.close().awaitCompletion() }
        resources.second?.close()?.awaitCompletion()
        eventLoopGroup.shutdownGracefully().awaitCompletion()
    }

    private suspend fun connection(peerId: String): QuicChannel = mutex.withLock {
        check(!closed) { "Raft RPC channel is closed" }
        connections[peerId]?.takeIf { it.isActive }?.let { return it }
        connections.remove(peerId)?.close()

        val address = peerResolver.resolve(peerId)
        require(address.port != 0) { "Raft peer port must not be zero: $peerId" }
        val datagram = datagramChannel()
        val connection = QuicChannel.newBootstrap(datagram)
            .streamHandler(object : ChannelInboundHandlerAdapter() {
                override fun channelActive(context: ChannelHandlerContext) {
                    context.close()
                }
            })
            .remoteAddress(InetSocketAddress(address.host, address.port))
            .connect()
            .awaitResult()
        connectionCreationCount++
        connections[peerId] = connection
        connection
    }

    private suspend fun datagramChannel(): Channel {
        datagramChannel?.takeIf { it.isActive }?.let { return it }
        val handler = QuicClientCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(idleTimeoutMillis, TimeUnit.MILLISECONDS)
            .initialMaxData(MAX_CONNECTION_DATA)
            .initialMaxStreamDataBidirectionalLocal(maximumFrameSize())
            .build()
        return Bootstrap()
            .group(eventLoopGroup)
            .channel(NioDatagramChannel::class.java)
            .handler(handler)
            .bind(0)
            .let { future ->
                future.awaitCompletion()
                future.channel()
            }
            .also { channel -> datagramChannel = channel }
    }

    private suspend fun evict(peerId: String, connection: QuicChannel) {
        mutex.withLock {
            if (connections[peerId] === connection) connections.remove(peerId)
        }
    }

    private fun maximumFrameSize(): Long =
        codec.headerSize.toLong() + codec.maxPayloadLength.toLong()

    private class ResponseHandler(
        codec: RaftRpcCodec,
        private val response: CompletableDeferred<ByteArray>,
    ) : NettyRaftRpcFrameHandler(codec) {
        override fun onFrame(context: ChannelHandlerContext, frame: ByteArray) {
            response.complete(frame)
        }

        override fun onFailure(cause: Throwable) {
            response.completeExceptionally(cause)
        }
    }

    companion object {
        const val ALPN = "fortis-raft/1"
        private const val DEFAULT_IDLE_TIMEOUT_MILLIS = 5_000L
        private const val MAX_CONNECTION_DATA = 64L * 1024L * 1024L
    }
}
