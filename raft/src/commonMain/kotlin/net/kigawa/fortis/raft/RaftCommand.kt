package net.kigawa.fortis.raft

import net.kigawa.fortis.storage.engine.FortisStorageEngine

sealed interface RaftCommand {
    suspend fun execute(storageEngine: FortisStorageEngine)
    data class Put(
        val key: ByteArray,
        val value: ByteArray,
    ) : RaftCommand {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || this::class != other::class) return false

            other as Put

            if (!key.contentEquals(other.key)) return false
            if (!value.contentEquals(other.value)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = key.contentHashCode()
            result = 31 * result + value.contentHashCode()
            return result
        }

        override suspend fun execute(storageEngine: FortisStorageEngine) {
            storageEngine.put(key, value)
        }
    }

    data class Delete(
        val key: ByteArray,
    ) : RaftCommand {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || this::class != other::class) return false

            other as Delete

            return key.contentEquals(other.key)
        }

        override fun hashCode(): Int {
            return key.contentHashCode()
        }

        override suspend fun execute(storageEngine: FortisStorageEngine) {
            storageEngine.delete(key)
        }
    }
}