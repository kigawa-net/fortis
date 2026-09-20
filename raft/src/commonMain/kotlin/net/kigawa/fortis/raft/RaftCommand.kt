package net.kigawa.fortis.raft

sealed interface RaftCommand {
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
    }

    data class Delete(
        val key: ByteArray,
    ) : RaftCommand {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other == null || this::class != other::class) return false

            other as Delete

            if (!key.contentEquals(other.key)) return false

            return true
        }

        override fun hashCode(): Int {
            return key.contentHashCode()
        }
    }
}