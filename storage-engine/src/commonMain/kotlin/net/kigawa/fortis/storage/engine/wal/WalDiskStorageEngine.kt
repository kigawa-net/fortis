package net.kigawa.fortis.storage.engine.wal

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.memory.MemoryStorageEngine
import net.kigawa.fortis.storage.engine.wal.codec.WalOperation
import net.kigawa.fortis.storage.engine.wal.codec.WalRecord

class WalDiskStorageEngine(
    private val wal: Wal,
    private val memory: MemoryStorageEngine = MemoryStorageEngine(),
    private var nextSequence: Long,
): FortisStorageEngine {
    private val mutex: Mutex = Mutex()
    private var failed: Throwable? = null

    companion object {
        val builder = ::WalDiskStorageEngineBuilder
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

            val sequence = nextSequence()
            val record = WalRecord(
                sequence = sequence,
                operation = WalOperation.PUT,
                key = key,
                value = value,
            )

            persist(record)
            memory.put(key, value)
        }
    }

    override suspend fun delete(
        key: ByteArray,
    ): Boolean {
        return mutex.withLock {
            checkHealthy()

            if (memory.get(key) == null) {
                return@withLock false
            }

            val record = WalRecord(
                sequence = nextSequence(),
                operation = WalOperation.DELETE,
                key = key,
                value = null,
            )

            persist(record)
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

    private suspend fun persist(
        record: WalRecord,
    ) {
        try {
            wal.append(record)
            wal.sync()
        } catch (e: Throwable) {
            failed = e
            throw e
        }
    }

    private fun nextSequence(): Long {
        check(nextSequence < Long.MAX_VALUE) {
            "WAL sequence exhausted"
        }

        return nextSequence++
    }
}