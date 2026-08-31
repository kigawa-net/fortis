package net.kigawa.fortis.storage.engine.wal

data class WalRecord(
    val sequence: Long,
    val operation: WalOperation,
    val key: ByteArray,
    val value: ByteArray?,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || this::class != other::class) return false

        other as WalRecord

        if (sequence != other.sequence) return false
        if (operation != other.operation) return false
        if (!key.contentEquals(other.key)) return false
        if (!value.contentEquals(other.value)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sequence.hashCode()
        result = 31 * result + operation.hashCode()
        result = 31 * result + key.contentHashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}