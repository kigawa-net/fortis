package net.kigawa.fortis.raft.node

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds

fun interface RaftElectionTimeoutProvider {
    fun next(): Duration
}

class RandomizedRaftElectionTimeoutProvider(
    private val minimum: Duration,
    private val maximum: Duration,
    private val random: Random = Random.Default,
) : RaftElectionTimeoutProvider {
    init {
        require(minimum > Duration.ZERO) {
            "Minimum election timeout must be positive"
        }
        require(maximum >= minimum) {
            "Maximum election timeout must not be less than minimum"
        }
        require(maximum.inWholeNanoseconds < Long.MAX_VALUE) {
            "Maximum election timeout is too large"
        }
    }

    override fun next(): Duration {
        val minimumNanos = minimum.inWholeNanoseconds
        val rangeNanos = maximum.inWholeNanoseconds - minimumNanos
        if (rangeNanos == 0L) return minimum
        return (minimumNanos + random.nextLong(rangeNanos + 1)).nanoseconds
    }
}
