package net.kigawa.fortis.raft.node

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class CoroutineRaftTimerTest {
    @Test
    fun electionTimeoutFiresAutomatically() = runTest {
        val events = mutableListOf<RaftTimeoutEvent>()
        val timer = timer(events)

        timer.reset(RaftTimeoutEvent.Election)
        advanceTimeBy(99.milliseconds)
        runCurrent()
        assertEquals(emptyList<RaftTimeoutEvent>(), events)

        advanceTimeBy(1.milliseconds)
        runCurrent()
        assertEquals(listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election), events)
        timer.cancel()
    }

    @Test
    fun heartbeatTimeoutFiresAutomatically() = runTest {
        val events = mutableListOf<RaftTimeoutEvent>()
        val timer = timer(events)

        timer.reset(RaftTimeoutEvent.Heartbeat)
        advanceTimeBy(25.milliseconds)
        runCurrent()

        assertEquals(listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Heartbeat), events)
        timer.cancel()
    }

    @Test
    fun resetCancelsPreviousSchedule() = runTest {
        val events = mutableListOf<RaftTimeoutEvent>()
        val timer = timer(events)
        timer.reset(RaftTimeoutEvent.Election)
        advanceTimeBy(50.milliseconds)

        timer.reset(RaftTimeoutEvent.Election)
        advanceTimeBy(50.milliseconds)
        runCurrent()
        assertEquals(emptyList<RaftTimeoutEvent>(), events)

        advanceTimeBy(50.milliseconds)
        runCurrent()
        assertEquals(listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Election), events)
        timer.cancel()
    }

    @Test
    fun changingTimerTypeCancelsPreviousSchedule() = runTest {
        val events = mutableListOf<RaftTimeoutEvent>()
        val timer = timer(events)
        timer.reset(RaftTimeoutEvent.Election)
        advanceTimeBy(50.milliseconds)

        timer.reset(RaftTimeoutEvent.Heartbeat)
        advanceTimeBy(25.milliseconds)
        runCurrent()

        assertEquals(listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Heartbeat), events)
        advanceTimeBy(25.milliseconds)
        runCurrent()
        assertEquals(listOf<RaftTimeoutEvent>(RaftTimeoutEvent.Heartbeat), events)
        timer.cancel()
    }

    @Test
    fun cancelPreventsTimeout() = runTest {
        val events = mutableListOf<RaftTimeoutEvent>()
        val timer = timer(events)
        timer.reset(RaftTimeoutEvent.Election)

        timer.cancel()
        advanceTimeBy(100.milliseconds)
        runCurrent()

        assertEquals(emptyList<RaftTimeoutEvent>(), events)
    }

    @Test
    fun randomizedProviderStaysWithinConfiguredRange() {
        val provider = RandomizedRaftElectionTimeoutProvider(
            minimum = 150.milliseconds,
            maximum = 300.milliseconds,
            random = Random(1),
        )

        repeat(100) {
            assertTrue(provider.next() in 150.milliseconds..300.milliseconds)
        }
    }

    private fun kotlinx.coroutines.test.TestScope.timer(
        events: MutableList<RaftTimeoutEvent>,
    ) = CoroutineRaftTimer(
        scope = this,
        electionTimeoutProvider = {
            100.milliseconds
        },
        heartbeatInterval = 25.milliseconds,
        onTimeout = events::add,
    )
}
