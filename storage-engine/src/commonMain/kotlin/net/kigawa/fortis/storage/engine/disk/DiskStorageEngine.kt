package net.kigawa.fortis.storage.engine.disk

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.WalOperation
import net.kigawa.fortis.storage.engine.wal.WalRecord

class DiskStorageEngine(
    private val wal: Wal,
    private val memory: MemoryStorageEngine = MemoryStorageEngine(),
    private var nextSequence: Long,
): FortisStorageEngine {
    private val mutex = Mutex()

    companion object {
        val builder = ::DiskStorageEngineBuilder
    }

    override suspend fun get(
        key: ByteArray,
    ): ByteArray? {
        return memory.get(key)
    }

    override suspend fun put(
        key: ByteArray,
        value: ByteArray,
    ) {
        mutex.withLock {
            val record = WalRecord(
                sequence = nextSequence++,
                operation = WalOperation.PUT,
                key = key,
                value = value,
            )

            wal.append(record)
            wal.sync()

            memory.put(key, value)
        }
    }

    override suspend fun delete(
        key: ByteArray,
    ): Boolean {
        return mutex.withLock {
            if (memory.get(key) == null) {
                return@withLock false
            }

            val record = WalRecord(
                sequence = nextSequence++,
                operation = WalOperation.DELETE,
                key = key,
                value = null,
            )

            wal.append(record)
            wal.sync()

            memory.delete(key)
        }
    }
}