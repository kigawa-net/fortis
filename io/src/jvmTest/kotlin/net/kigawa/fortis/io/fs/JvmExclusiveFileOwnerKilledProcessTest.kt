package net.kigawa.fortis.io.fs

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertFailsWith

/**
 * JVM-only process-death test. The lock is an OS resource and must be released
 * even when the holder does not run Kotlin finally/AutoCloseable.close().
 */
class JvmExclusiveFileOwnerKilledProcessTest {
    @Test
    fun forciblyKilledOwnerAllowsNewReplicaOwner() {
        val directory = Files.createTempDirectory("fortis-writer-killed-")
        val source = Files.createTempFile("FortisHeldLock-", ".java")
        var child: Process? = null
        try {
            Files.writeString(source, """
                import java.nio.channels.FileChannel;
                import java.nio.channels.FileLock;
                import java.nio.file.Path;
                import java.nio.file.StandardOpenOption;
                class HeldLock {
                    public static void main(String[] args) throws Exception {
                        try (FileChannel channel = FileChannel.open(
                            Path.of(args[0], ".fortis-writer.lock"),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE
                        ); FileLock lock = channel.lock(0L, 1L, false)) {
                            System.out.println("LOCKED");
                            System.out.flush();
                            Thread.sleep(120000);
                        }
                    }
                }
            """.trimIndent())
            val java = Path.of(System.getProperty("java.home"), "bin", "java").toString()
            child = ProcessBuilder(java, source.toString(), directory.toString())
                .redirectErrorStream(true).start()
            val ready = child.inputStream.bufferedReader().readLine()
            check(ready == "LOCKED") { "Lock holder failed to start: $ready" }
            assertFailsWith<WriterAlreadyOwned> {
                JvmExclusiveFileOwner.tryAcquire(directory)
            }
            child.destroyForcibly()
            check(child.waitFor(15, TimeUnit.SECONDS)) { "Killed child did not exit" }
            JvmExclusiveFileOwner.tryAcquire(directory).use { /* acquired after OS unlock */ }
        } finally {
            child?.destroyForcibly()
            child?.waitFor(15, TimeUnit.SECONDS)
            Files.deleteIfExists(source)
            Files.deleteIfExists(directory.resolve(".fortis-writer.lock"))
            Files.deleteIfExists(directory)
        }
    }
}
