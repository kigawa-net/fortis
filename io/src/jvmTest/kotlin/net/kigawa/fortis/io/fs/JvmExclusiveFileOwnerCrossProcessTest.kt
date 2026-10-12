package net.kigawa.fortis.io.fs

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Separate JVM entry point: returns 23 for lock contention and 0 for ownership.
 * Never mutates the replica's WAL/log.
 */
object JvmWriterLockProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 1)
        val code = try {
            JvmExclusiveFileOwner.tryAcquire(Path.of(args[0])).use {}
            0
        } catch (_: WriterAlreadyOwned) {
            23
        }
        kotlin.system.exitProcess(code)
    }
}

class JvmExclusiveFileOwnerCrossProcessTest {
    private fun probe(directory: Path): Int {
        val java = Path.of(System.getProperty("java.home"), "bin", "java").toString()
        val classpath = System.getProperty("java.class.path")
        val process = ProcessBuilder(
            java, "-cp", classpath,
            JvmWriterLockProbe::class.java.name,
            directory.toString(),
        ).redirectErrorStream(true).start()
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            error("Child JVM timed out acquiring writer ownership")
        }
        val output = process.inputStream.bufferedReader().readText()
        val exit = process.exitValue()
        check(exit == 0 || exit == 23) { "Child JVM failed ($exit): $output" }
        return exit
    }

    @Test
    fun secondJvmCannotOwnUntilFirstReleases() {
        val directory = Files.createTempDirectory("fortis-owner-process-")
        try {
            JvmExclusiveFileOwner.tryAcquire(directory).use {
                assertEquals(23, probe(directory), "A second process must not own the same replica")
                assertTrue(Files.exists(directory.resolve(".fortis-writer.lock")))
            }
            assertEquals(0, probe(directory), "OS lock must be released after close")
        } finally {
            Files.deleteIfExists(directory.resolve(".fortis-writer.lock"))
            Files.deleteIfExists(directory)
        }
    }
}
