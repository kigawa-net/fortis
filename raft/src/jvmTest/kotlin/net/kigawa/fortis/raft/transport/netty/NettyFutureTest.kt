package net.kigawa.fortis.raft.transport.netty

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.util.concurrent.ImmediateEventExecutor
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFalse

@OptIn(ExperimentalCoroutinesApi::class)
class NettyFutureTest {
    @Test
    fun closesChannelWhenCancellationWinsOverResultDelivery() = runTest {
        val promise = ImmediateEventExecutor.INSTANCE.newPromise<EmbeddedChannel>()
        val channel = EmbeddedChannel()
        try {
            val request = launch { promise.awaitResult() }
            runCurrent()
            promise.setSuccess(channel)
            request.cancel()
            runCurrent()
            assertFalse(channel.isOpen)
        } finally {
            channel.finishAndReleaseAll()
        }
    }

    @Test
    fun closesChannelCreatedAfterCallerCancellation() = runTest {
        val promise = ImmediateEventExecutor.INSTANCE.newPromise<EmbeddedChannel>()
        val channel = EmbeddedChannel()
        try {
            val request = launch { promise.awaitResult() }
            runCurrent()
            request.cancel()
            runCurrent()
            assertFalse(promise.isCancelled)
            promise.setSuccess(channel)
            runCurrent()
            assertFalse(channel.isOpen)
        } finally {
            channel.finishAndReleaseAll()
        }
    }
}
