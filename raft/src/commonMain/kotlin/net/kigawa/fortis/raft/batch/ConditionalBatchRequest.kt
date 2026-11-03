package net.kigawa.fortis.raft.batch

import net.kigawa.fortis.storage.engine.ByteArrayKey

/**
 * 単一 Raft Group 内の前提条件。
 *
 * revision は「そのキーに最後に書き込んだ Raft log index」（削除マーカーも含む）であり、
 * 未書き込みのキーは 0 である。版は単調増加するためキー単位の CAS に使える。
 */
sealed interface KvCondition {
    val key: ByteArray

    /** キーが存在しないこと（削除済みも不在として扱う）。 */
    class Absent(key: ByteArray) : KvCondition {
        override val key: ByteArray = key.copyOf()

        override fun equals(other: Any?): Boolean =
            other is Absent && key.contentEquals(other.key)

        override fun hashCode(): Int = key.contentHashCode()

        override fun toString(): String = "Absent(key=${key.contentToString()})"
    }

    /** キーの現在 revision が [revision] と一致すること。 */
    class RevisionEquals(key: ByteArray, val revision: Long) : KvCondition {
        override val key: ByteArray = key.copyOf()

        init {
            require(revision >= 0) { "Revision must not be negative" }
        }

        override fun equals(other: Any?): Boolean =
            other is RevisionEquals && revision == other.revision && key.contentEquals(other.key)

        override fun hashCode(): Int = 31 * key.contentHashCode() + revision.hashCode()

        override fun toString(): String = "RevisionEquals(key=${key.contentToString()}, revision=$revision)"
    }
}

/** 単一 Raft Group 内の変更。Put/Delete とも適用時に当該 log index を revision として記録する。 */
sealed interface KvMutation {
    val key: ByteArray

    class Put(key: ByteArray, value: ByteArray) : KvMutation {
        override val key: ByteArray = key.copyOf()
        val value: ByteArray = value.copyOf()

        override fun equals(other: Any?): Boolean =
            other is Put && key.contentEquals(other.key) && value.contentEquals(other.value)

        override fun hashCode(): Int = 31 * key.contentHashCode() + value.contentHashCode()

        override fun toString(): String = "Put(key=${key.contentToString()}, size=${value.size})"
    }

    class Delete(key: ByteArray) : KvMutation {
        override val key: ByteArray = key.copyOf()

        override fun equals(other: Any?): Boolean =
            other is Delete && key.contentEquals(other.key)

        override fun hashCode(): Int = key.contentHashCode()

        override fun toString(): String = "Delete(key=${key.contentToString()})"
    }
}

/**
 * 条件付き Atomic Batch の要求。
 *
 * [principal] と [requestId] の組が受付記録（Request Receipt）のキーになる。
 * [requestDigest] は条件と変更の正規エンコードそのものであり、ハッシュではないため
 * 同一性は厳密に比較される。同じ [requestId] でも内容が異なる要求は別要求として拒否される。
 * 全キーは一意でなければならない（条件同士・変更同士の重複は不正）。
 */
class ConditionalBatchRequest(
    val principal: String,
    val requestId: String,
    conditions: List<KvCondition>,
    mutations: List<KvMutation>,
) {
    val conditions: List<KvCondition> = conditions.toList()
    val mutations: List<KvMutation> = mutations.toList()
    private val digest: ByteArray

    init {
        require(principal.isNotEmpty() && principal.length <= MAX_IDENTITY_LENGTH) {
            "Principal must be 1..$MAX_IDENTITY_LENGTH characters"
        }
        require(requestId.isNotEmpty() && requestId.length <= MAX_IDENTITY_LENGTH) {
            "Request ID must be 1..$MAX_IDENTITY_LENGTH characters"
        }
        require(mutations.isNotEmpty()) { "Conditional batch needs at least one mutation" }
        require(this.conditions.size <= MAX_ITEMS) { "Too many conditions: ${this.conditions.size}" }
        require(this.mutations.size <= MAX_ITEMS) { "Too many mutations: ${this.mutations.size}" }
        val conditionKeys = HashSet<ByteArrayKey>()
        for (condition in this.conditions) {
            checkKey(condition.key)
            require(conditionKeys.add(ByteArrayKey(condition.key))) {
                "Duplicate condition key is not allowed"
            }
        }
        val mutationKeys = HashSet<ByteArrayKey>()
        for (mutation in this.mutations) {
            checkKey(mutation.key)
            if (mutation is KvMutation.Put) {
                require(mutation.value.size <= MAX_VALUE_BYTES) { "Value exceeds $MAX_VALUE_BYTES bytes" }
            }
            require(mutationKeys.add(ByteArrayKey(mutation.key))) {
                "Duplicate mutation key is not allowed"
            }
        }
        digest = ConditionalBatchCodec.encodeRequestBody(this.conditions, this.mutations)
    }

    /** 正規化された要求本体（条件と変更）のバイト列。コピーを返す。 */
    val requestDigest: ByteArray
        get() = digest.copyOf()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ConditionalBatchRequest) return false
        return principal == other.principal &&
            requestId == other.requestId &&
            conditions == other.conditions &&
            mutations == other.mutations
    }

    override fun hashCode(): Int {
        var result = principal.hashCode()
        result = 31 * result + requestId.hashCode()
        result = 31 * result + conditions.hashCode()
        result = 31 * result + mutations.hashCode()
        return result
    }

    override fun toString(): String =
        "ConditionalBatchRequest(principal=$principal, requestId=$requestId, " +
            "conditions=${conditions.size}, mutations=${mutations.size})"

    private fun checkKey(key: ByteArray) {
        require(key.isNotEmpty() && key.size <= MAX_KEY_BYTES) {
            "Key must be 1..$MAX_KEY_BYTES bytes"
        }
    }

    companion object {
        const val MAX_IDENTITY_LENGTH: Int = 256
        const val MAX_ITEMS: Int = 64
        const val MAX_KEY_BYTES: Int = 1024
        const val MAX_VALUE_BYTES: Int = 1024 * 1024
    }
}

/** 単一 Group 外へ跨るキーを含む batch。この slice では実行せず拒否する。 */
class UnsupportedCrossGroupBatchException(
    val rangeIds: Set<String>,
) : Exception("Conditional batch spans multiple groups: $rangeIds")
