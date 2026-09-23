package net.kigawa.fortis.raft.transport.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.socket.ChannelInputShutdownReadComplete
import java.io.ByteArrayOutputStream
import net.kigawa.fortis.raft.transport.RaftTransportException
import net.kigawa.fortis.raft.transport.codec.RaftRpcCodec
import net.kigawa.fortis.raft.transport.codec.RaftRpcDecodeResult

internal abstract class NettyRaftRpcFrameHandler(
    private val codec: RaftRpcCodec,
) : ChannelInboundHandlerAdapter() {
    private val buffer = ByteArrayOutputStream()
    private var failed = false
    private var delivered = false

    final override fun channelRead(context: ChannelHandlerContext, message: Any) {
        val bytes = message as ByteBuf
        try {
            if (failed || delivered) return
            if (buffer.size().toLong() + bytes.readableBytes() > maximumFrameSize()) {
                fail(context, "Raft RPC frame is too large")
                return
            }
            val chunk = ByteArray(bytes.readableBytes())
            bytes.readBytes(chunk)
            buffer.write(chunk)
        } finally {
            bytes.release()
        }
    }

    final override fun userEventTriggered(context: ChannelHandlerContext, event: Any) {
        if (event == ChannelInputShutdownReadComplete.INSTANCE) {
            finish(context)
            return
        }
        context.fireUserEventTriggered(event)
    }

    final override fun exceptionCaught(context: ChannelHandlerContext, cause: Throwable) {
        if (!failed) {
            failed = true
            onFailure(cause)
        }
        context.close()
    }

    final override fun channelInactive(context: ChannelHandlerContext) {
        finish(context)
        context.fireChannelInactive()
    }

    protected abstract fun onFrame(context: ChannelHandlerContext, frame: ByteArray)

    protected abstract fun onFailure(cause: Throwable)

    private fun finish(context: ChannelHandlerContext) {
        if (failed || delivered) return
        val frame = buffer.toByteArray()
        when (val result = codec.decode(frame)) {
            is RaftRpcDecodeResult.Success -> {
                if (result.bytesRead != frame.size) {
                    fail(context, "Unexpected trailing bytes in Raft RPC frame")
                } else {
                    delivered = true
                    onFrame(context, frame)
                }
            }

            RaftRpcDecodeResult.Incomplete -> fail(context, "Incomplete Raft RPC frame")
            is RaftRpcDecodeResult.Corrupted -> fail(
                context,
                "Corrupted Raft RPC frame: ${result.reason}",
            )
        }
    }

    private fun fail(context: ChannelHandlerContext, message: String) {
        if (failed) return
        failed = true
        onFailure(RaftTransportException(message))
        context.close()
    }

    private fun maximumFrameSize(): Long =
        codec.headerSize.toLong() + codec.maxPayloadLength.toLong()
}
