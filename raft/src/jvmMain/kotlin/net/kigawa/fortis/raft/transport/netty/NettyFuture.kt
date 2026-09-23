package net.kigawa.fortis.raft.transport.netty

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

internal suspend fun <T> Future<T>.awaitResult(): T {
    awaitCompletion()
    return getNow()
}
