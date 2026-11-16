package net.kigawa.fortis.raft.logging

import kotlin.test.Test
import kotlin.test.assertEquals

class RaftLoggerTest {
    @Test
    fun recordingLoggerCapturesAllEntries() {
        val logger = RecordingLogger()

        logger.log(LogLevel.INFO, "comp", "event.a", mapOf("k" to "v"))
        logger.debug("comp", "event.b")
        logger.info("comp", "event.c")
        logger.warn("comp", "event.d")
        logger.error("comp", "event.e")

        assertEquals(5, logger.entries.size)
        assertEquals(StructuredLog(LogLevel.INFO, "comp", "event.a", mapOf("k" to "v")), logger.entries[0])
        assertEquals(StructuredLog(LogLevel.DEBUG, "comp", "event.b"), logger.entries[1])
        assertEquals(StructuredLog(LogLevel.INFO, "comp", "event.c"), logger.entries[2])
        assertEquals(StructuredLog(LogLevel.WARN, "comp", "event.d"), logger.entries[3])
        assertEquals(StructuredLog(LogLevel.ERROR, "comp", "event.e"), logger.entries[4])
    }

    @Test
    fun noOpLoggerOutputsNothing() {
        NoOpLogger.log(LogLevel.INFO, "comp", "event")
        NoOpLogger.debug("comp", "event")
        NoOpLogger.info("comp", "event")
        NoOpLogger.warn("comp", "event")
        NoOpLogger.error("comp", "event")
    }

    @Test
    fun noOpLoggerAsInterfaceOutputsNothing() {
        val logger: RaftLogger = NoOpLogger

        logger.log(LogLevel.INFO, "comp", "event")
        logger.debug("comp", "event")
        logger.info("comp", "event")
        logger.warn("comp", "event")
        logger.error("comp", "event")
    }
}
