package net.kigawa.fortis.storage.engine.wal

import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import net.kigawa.fortis.storage.engine.wal.codec.WalOperation

data class WalDiskStorageEngineBuilder(
    private val wal: Wal?,
    private val memory: MemoryStorageEngine?,
) {

    suspend fun build(): WalDiskStorageEngine {
        val wal = requireNotNull(wal) {
            "wal is required"
        }

        val memory =
            memory ?: MemoryStorageEngine()

        var maxSequence = 0L

        wal.replay { record ->
            when (record.operation) {
                WalOperation.PUT -> {
                    memory.put(
                        record.key,
                        requireNotNull(record.value),
                    )
                }

                WalOperation.DELETE -> {
                    memory.delete(record.key)
                }
            }

            if (record.sequence > maxSequence) {
                maxSequence = record.sequence
            }
        }
        check(maxSequence < Long.MAX_VALUE) {
            "WAL sequence exhausted"
        }
        return WalDiskStorageEngine(
            wal = wal,
            memory = memory,
            nextSequence = maxSequence + 1,
        )
    }
}
