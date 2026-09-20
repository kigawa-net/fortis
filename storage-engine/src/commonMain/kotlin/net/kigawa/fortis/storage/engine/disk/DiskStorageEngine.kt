package net.kigawa.fortis.storage.engine.disk

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import net.kigawa.fortis.storage.engine.wal.Wal
import net.kigawa.fortis.storage.engine.wal.WalOperation
import net.kigawa.fortis.storage.engine.wal.WalRecord

data class DiskStorageEngine(
    private val wal: Wal,
    private val memory: MemoryStorageEngine = MemoryStorageEngine(),
    private var nextSequence: Long,
    private val mutex: Mutex = Mutex(),
    private var failed: Throwable? = null,
): FortisStorageEngine {

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
            checkHealthy()

            val record = WalRecord(
                sequence = nextSequence++,
                operation = WalOperation.PUT,
                key = key,
                value = value,
            )
            try {
                wal.append(record)
                wal.sync()
            } catch (e: Throwable) {
                failed = e
                throw e
            }
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

    private fun checkHealthy() {
        failed?.let {
            throw IllegalStateException(
                "DiskStorageEngine is in failed state",
                it,
            )
        }
    }
}