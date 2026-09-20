package net.kigawa.fortis.storage.engine.disk

import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.WalOperation

data class DiskStorageEngineBuilder(
    private var wal: Wal? = null,
    private var memory: MemoryStorageEngine? = null,
) {

    fun wal(
        wal: Wal,
    ): DiskStorageEngineBuilder = apply {
        this.wal = wal
    }

    fun memory(
        memory: MemoryStorageEngine,
    ): DiskStorageEngineBuilder = apply {
        this.memory = memory
    }

    suspend fun build(): DiskStorageEngine {
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
        return DiskStorageEngine(
            wal = wal,
            memory = memory,
            nextSequence = maxSequence + 1,
        )
    }
}
