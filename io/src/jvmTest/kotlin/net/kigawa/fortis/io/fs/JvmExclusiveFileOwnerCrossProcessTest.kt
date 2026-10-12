package net.kigawa.fortis.io.fs

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Cross-process test intentionally uses Java single-file source launch, not the Gradle worker classpath. */
class JvmExclusiveFileOwnerCrossProcessTest {
    private fun probe(directory: Path): Int {
        val java = Path.of(System.getProperty("java.home"), "bin", "java").toString()
        val source = Files.createTempFile("FortisLockProbe-", ".java")
        try {
            Files.writeString(source, """
                import java.nio.channels.FileChannel;
                import java.nio.channels.FileLock;
                import java.nio.file.Path;
                import java.nio.file.StandardOpenOption;
                class LockProbe {
                    public static void main(String[] args) throws Exception {
                        try (FileChannel channel = FileChannel.open(
                            Path.of(args[0], ".fortis-writer.lock"),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                            FileLock lock = channel.tryLock(0L, 1L, false);
                            if (lock == null) System.exit(23);
                            try (lock) { }
                        }
                    }
                }
            """.trimIndent())
            val process = ProcessBuilder(java, source.toString(), directory.toString())
                .redirectErrorStream(true)
                .start()
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly()
                error("Child JVM timed out acquiring writer ownership")
            }
            val output = process.inputStream.bufferedReader().readText()
            val exit = process.exitValue()
            check(exit == 0 || exit == 23) { "Child JVM failed ($exit): $output" }
            return exit
        } finally {
            Files.deleteIfExists(source)
        }
    }

    @Test
    fun secondJvmCannotOwnUntilFirstReleases() {
        val directory = Files.createTempDirectory("fortis-owner-process-")
        try {
            JvmExclusiveFileOwner.tryAcquire(directory).use {
                assertEquals(23, probe(directory), "A second JVM must not own the same replica")
                assertTrue(Files.exists(directory.resolve(".fortis-writer.lock")))
            }
            assertEquals(0, probe(directory), "OS lock must be released after close")
        } finally {
            Files.deleteIfExists(directory.resolve(".fortis-writer.lock"))
            Files.deleteIfExists(directory)
        }
    }
}
