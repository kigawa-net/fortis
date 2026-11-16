package net.kigawa.fortis.raft.transport.netty

import io.netty.channel.Channel
import io.netty.util.concurrent.Future
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

internal suspend fun Future<*>.awaitCompletion(): Unit =
    suspendCancellableCoroutine { continuation ->
        addListener { future ->
            if (future.isSuccess) {
                continuation.resumeWith(Result.success(Unit))
            } else {
                continuation.resumeWithException(
                    future.cause() ?: IllegalStateException("Netty operation failed"),
                )
            }
        }
        continuation.invokeOnCancellation { cancel(false) }
    }

internal suspend fun <T : Channel> Future<T>.awaitResult(): T =
    suspendCancellableCoroutine { continuation ->
        addListener { future ->
            if (future.isSuccess) {
                continuation.resume(getNow()) { _, channel, _ -> channel.close() }
            } else {
                continuation.resumeWithException(
                    future.cause() ?: IllegalStateException("Netty operation failed"),
                )
            }
        }
        // Channel creation may finish after cancellation; close the result rather
        // than cancelling the promise and losing ownership of that channel.
    }
