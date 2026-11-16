package net.kigawa.fortis.raft.node

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration

class CoroutineRaftTimer(
    private val scope: CoroutineScope,
    private val electionTimeoutProvider: RaftElectionTimeoutProvider,
    private val heartbeatInterval: Duration,
    private val onTimeout: suspend (RaftTimeoutEvent) -> Unit,
) : RaftTimer {
    private val mutex = Mutex()
    private var generation = 0L
    private var scheduledJob: Job? = null

    init {
        require(heartbeatInterval > Duration.ZERO) {
            "Heartbeat interval must be positive"
        }
    }

    override suspend fun reset(event: RaftTimeoutEvent) {
        val timeout = when (event) {
            RaftTimeoutEvent.Election -> electionTimeoutProvider.next()
            RaftTimeoutEvent.Heartbeat -> heartbeatInterval
        }
        require(timeout > Duration.ZERO) {
            "Raft timeout must be positive"
        }

        mutex.withLock {
            scheduledJob?.cancel()
            val scheduledGeneration = ++generation
            scheduledJob = scope.launch {
                delay(timeout)
                val shouldFire = mutex.withLock {
                    if (generation != scheduledGeneration) {
                        false
                    } else {
                        scheduledJob = null
                        true
                    }
                }
                if (shouldFire) onTimeout(event)
            }
        }
    }

    override suspend fun cancel() {
        mutex.withLock {
            generation++
            scheduledJob?.cancel()
            scheduledJob = null
        }
    }
}
