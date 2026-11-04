package net.kigawa.fortis.raft

import net.kigawa.fortis.raft.batch.ConditionalBatchRequest
import net.kigawa.fortis.storage.engine.FortisStorageEngine
import net.kigawa.fortis.storage.engine.mvcc.VersionedFortisStorageEngine

sealed interface RaftCommand {
    suspend fun execute(storageEngine: FortisStorageEngine)

    // version 付き実行。対応ストレージなら版指定で適用し、非対応なら既存 execute に退避する。
    suspend fun executeAt(storageEngine: FortisStorageEngine, version: Long) {
        val versioned = storageEngine as? VersionedFortisStorageEngine
        if (versioned == null) {
            execute(storageEngine)
            return
        }
        when (this) {
            is Put -> versioned.putAt(key, value, version)
            is Delete -> versioned.deleteAt(key, version)
            is ConditionalBatch -> throw IllegalStateException(CONDITIONAL_BATCH_REQUIRES_STATE_MACHINE)
        }
    }

    /**
     * 条件付き batch。適用は受付記録と一体で [StorageEngineStateMachine] が行うため、
     * storage への直接適用はできない（execute/executeAt は失敗する）。
     */
    data class ConditionalBatch(
        val request: ConditionalBatchRequest,
    ) : RaftCommand {
        override suspend fun execute(storageEngine: FortisStorageEngine) {
            throw IllegalStateException(CONDITIONAL_BATCH_REQUIRES_STATE_MACHINE)
        }

        override suspend fun executeAt(storageEngine: FortisStorageEngine, version: Long) {
            throw IllegalStateException(CONDITIONAL_BATCH_REQUIRES_STATE_MACHINE)
        }
    }
    /**
     * 単一キーへの put。key/value は構築時に防御コピーし、getter も毎回コピーを返す。
     * 呼び出し側が元配列を書き換えても、log/apply/replication で観測される内容は変わらない。
     */
    class Put(
        key: ByteArray,
        value: ByteArray,
    ) : RaftCommand {
        private val keyBytes: ByteArray = key.copyOf()
        private val valueBytes: ByteArray = value.copyOf()

        val key: ByteArray
            get() = keyBytes.copyOf()

        val value: ByteArray
            get() = valueBytes.copyOf()

        fun copy(key: ByteArray = this.key, value: ByteArray = this.value): Put = Put(key, value)

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || this::class != other::class) return false

            other as Put

            if (!keyBytes.contentEquals(other.keyBytes)) return false
            if (!valueBytes.contentEquals(other.valueBytes)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = keyBytes.contentHashCode()
            result = 31 * result + valueBytes.contentHashCode()
            return result
        }

        override fun toString(): String =
            "Put(key=${keyBytes.contentToString()}, value=${valueBytes.contentToString()})"

        override suspend fun execute(storageEngine: FortisStorageEngine) {
            storageEngine.put(keyBytes.copyOf(), valueBytes.copyOf())
        }
    }

    /**
     * 単一キーへの delete。key は構築時に防御コピーし、getter も毎回コピーを返す。
     */
    class Delete(
        key: ByteArray,
    ) : RaftCommand {
        private val keyBytes: ByteArray = key.copyOf()

        val key: ByteArray
            get() = keyBytes.copyOf()

        fun copy(key: ByteArray = this.key): Delete = Delete(key)

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || this::class != other::class) return false

            other as Delete

            return keyBytes.contentEquals(other.keyBytes)
        }

        override fun hashCode(): Int {
            return keyBytes.contentHashCode()
        }

        override fun toString(): String = "Delete(key=${keyBytes.contentToString()})"

        override suspend fun execute(storageEngine: FortisStorageEngine) {
            storageEngine.delete(keyBytes.copyOf())
        }
    }
}

private const val CONDITIONAL_BATCH_REQUIRES_STATE_MACHINE =
    "ConditionalBatch must be applied through StorageEngineStateMachine.applyAt"