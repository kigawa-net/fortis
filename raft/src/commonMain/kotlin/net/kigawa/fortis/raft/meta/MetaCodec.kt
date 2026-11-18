package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.sharding.KeyRange
import net.kigawa.fortis.raft.sharding.RangeMetadata
import net.kigawa.fortis.raft.sharding.RangeState

internal class MetaWriter {
    private val fields = ArrayList<ByteArray>()

    fun long(value: Long) {
        fields.add(ByteArray(8) { offset -> (value ushr ((7 - offset) * 8)).toByte() })
    }

    fun bytes(value: ByteArray?) {
        long(value?.size?.toLong() ?: -1)
        if (value != null) fields.add(value)
    }

    fun string(value: String) = bytes(value.encodeToByteArray())

    fun node(node: RegisteredNode) {
        string(node.nodeId)
        string(node.address)
        string(node.state.name)
        long(node.lastHeartbeat)
    }

    fun range(range: RangeMetadata) {
        string(range.rangeId.value)
        bytes(range.keyRange.startInclusive)
        bytes(range.keyRange.endExclusive)
        long(range.epoch)
        string(range.state.name)
    }

    fun encode(): ByteArray {
        val size = fields.sumOf { it.size.toLong() }
        require(size <= Int.MAX_VALUE) { "Meta Raft payload is too large" }
        val result = ByteArray(size.toInt())
        var offset = 0
        for (field in fields) {
            field.copyInto(result, offset)
            offset += field.size
        }
        return result
    }
}

internal class MetaReader(private val data: ByteArray) {
    private var offset = 0

    fun long(): Long {
        require(data.size - offset >= 8) { "Truncated Meta Raft payload" }
        var value = 0L
        repeat(8) { value = (value shl 8) or (data[offset++].toLong() and 0xff) }
        return value
    }

    fun count(): Int {
        val value = long()
        require(value in 0..(data.size - offset).toLong()) { "Invalid Meta Raft length" }
        return value.toInt()
    }

    fun bytes(): ByteArray? {
        val size = long()
        if (size == -1L) return null
        require(size in 0..(data.size - offset).toLong()) { "Invalid Meta Raft field length" }
        val end = offset + size.toInt()
        return data.copyOfRange(offset, end).also { offset = end }
    }

    fun string(): String = requireNotNull(bytes()).decodeToString(throwOnInvalidSequence = true)

    fun node(): RegisteredNode = RegisteredNode(string(), string(), RegisteredNodeState.valueOf(string()), long())

    fun range(): RangeMetadata =
        RangeMetadata(KeyRange(string(), bytes(), bytes()), long(), RangeState.valueOf(string()))

    fun requireEnd() = require(offset == data.size) { "Trailing bytes in Meta Raft payload" }
}

internal fun MetaRaftSnapshot.encode(): ByteArray = MetaWriter().apply {
    long(1)
    val config = membership
    long(config?.version ?: -1)
    if (config != null) {
        long(config.members.size.toLong())
        for (member in config.members.values.sortedBy { it.memberId }) {
            string(member.memberId)
            string(member.role.name)
            string(member.status.name)
        }
    }
    bytes(pendingPromotion?.encodeToByteArray())
    long(nodes.size.toLong())
    nodes.values.sortedBy { it.nodeId }.forEach(::node)
    long(ranges.size.toLong())
    ranges.values.sortedBy { it.rangeId.value }.forEach(::range)
}.encode()

internal fun decodeMetaSnapshot(data: ByteArray): MetaRaftSnapshot = with(MetaReader(data)) {
    require(long() == 1L) { "Unsupported Meta Raft snapshot format" }
    val version = long()
    val membership = if (version == -1L) null else {
        val memberCount = count()
        val members = buildMap {
            repeat(memberCount) {
                val member = ClusterMember(string(), ClusterMemberRole.valueOf(string()), ClusterMemberStatus.valueOf(string()))
                require(put(member.memberId, member) == null)
            }
        }
        ClusterMembership(version, members)
    }
    val pendingPromotion = bytes()?.decodeToString(throwOnInvalidSequence = true)
    if (pendingPromotion != null) {
        require(membership?.members?.get(pendingPromotion)?.role == ClusterMemberRole.LEARNER)
    }
    val nodeCount = count()
    val nodes = buildMap {
        repeat(nodeCount) {
            val node = node()
            require(put(node.nodeId, node) == null)
        }
    }
    val rangeCount = count()
    val ranges = buildMap {
        repeat(rangeCount) {
            val range = range()
            require(put(range.rangeId, range) == null)
        }
    }
    requireEnd()
    MetaRaftSnapshot(membership, nodes, ranges, pendingPromotion)
}
