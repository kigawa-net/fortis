package net.kigawa.fortis.raft.sharding

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.batch.UnsupportedCrossGroupBatchException
import net.kigawa.fortis.raft.scan.ScanEntry

/** 単一レンジを担当する Raft グループの識別子。 */
data class GroupHandle(
    val rangeId: String,
)

/**
 * 不変のレンジ構成から経路を計算する。構成変更時は再構築する。
 */
class MultiRaftRouter private constructor(
    val table: RangeTable,
    groups: Map<String, GroupHandle>,
    private val epochs: Map<String, Long>,
) {
    constructor(table: RangeTable, groups: Map<String, GroupHandle>) : this(table, groups, emptyMap())

    val groups: Map<String, GroupHandle> = groups.toMap()

    init {
        val tableIds = table.ranges.map { it.rangeId }.toSet()
        require(groups.keys == tableIds) {
            "groups must cover exactly the table ranges: table=$tableIds groups=${groups.keys}"
        }
        for ((id, handle) in groups) {
            require(handle.rangeId == id) {
                "GroupHandle.rangeId mismatch: key=$id handle=${handle.rangeId}"
            }
        }
    }

    /** [key] を担当するグループハンドルを返す。 */
    fun route(key: ByteArray): GroupHandle {
        val range = table.route(key)
        return checkNotNull(groups[range.rangeId]) {
            "no group for range ${range.rangeId}"
        }
    }

    /** [key] を担当する rangeId を返す。 */
    fun rangeIdOf(key: ByteArray): String = route(key).rangeId

    /** [RaftCommand] の対象キーから担当グループを返す。 */
    fun routeCommand(command: RaftCommand): GroupHandle = when (command) {
        is RaftCommand.Put -> route(command.key)
        is RaftCommand.Delete -> route(command.key)
        is RaftCommand.ConditionalBatch -> routeBatch(command)
    }

    /** キーの担当グループと要求に添付する世代を返す。 */
    fun routeWithEpoch(key: ByteArray): RangeRoute = routeWithEpoch(route(key))

    /** 半開区間を担当レンジごとの半開区間へ切り分ける。 */
    fun planScan(startInclusive: ByteArray?, endExclusive: ByteArray?): List<RangeScanSlice> {
        val start = startInclusive?.copyOf()
        val end = endExclusive?.copyOf()
        if (start != null && end != null) {
            val order = KeyOrder.compare(start, end)
            require(order <= 0) { "scan start must not exceed end" }
            if (order == 0) return emptyList()
        }
        // The empty byte array is the minimum key.
        if (end?.isEmpty() == true) return emptyList()
        val slices = mutableListOf<RangeScanSlice>()
        val first = if (start == null) 0 else table.ranges.indexOf(table.route(start))
        for (index in first until table.ranges.size) {
            val range = table.ranges[index]
            val lower = if (index == first && start != null) start else range.startInclusive
            if (lower != null && end != null && KeyOrder.compare(lower, end) >= 0) break
            val upper = range.endExclusive
            val clippedEnd = if (end != null && (upper == null || KeyOrder.compare(end, upper) <= 0)) end else upper
            if (clippedEnd?.isEmpty() != true) {
                slices += RangeScanSlice(
                    routeWithEpoch(groups.getValue(range.rangeId)),
                    KeyRange(range.rangeId, lower, clippedEnd),
                )
            }
        }
        return slices
    }

    /**
     * 各 shard に先頭から最大で残り件数の取得を依頼し、キー順に結合する。
     * 読み取りは shard ごとに独立し、revision は各グループに属する。
     */
    suspend fun scan(
        startInclusive: ByteArray?,
        endExclusive: ByteArray?,
        limit: Int,
        query: suspend (RangeScanSlice, Int) -> RangeResponse<List<ScanEntry>>,
    ): RangeResponse<List<ScanEntry>> {
        require(limit > 0) { "scan limit must be positive" }
        val entries = mutableListOf<ScanEntry>()
        for (slice in planScan(startInclusive, endExclusive)) {
            val remaining = limit - entries.size
            when (val response = query(slice, remaining)) {
                is RangeResponse.Success -> {
                    val page = response.value
                    check(page.size <= remaining && page.all { slice.bounds.contains(it.key) } &&
                        page.zipWithNext().all { (a, b) -> KeyOrder.compare(a.key, b.key) < 0 }) {
                        "Invalid scan result for ${slice.route.rangeId.value}"
                    }
                    entries.addAll(page)
                    if (entries.size == limit) break
                }
                is RedirectResponse -> return response
                is RangeResponse.LeaderUnavailable -> return response
            }
        }
        return RangeResponse.Success(entries)
    }

    private fun routeWithEpoch(group: GroupHandle): RangeRoute = RangeRoute(group, epochs[group.rangeId] ?: 0)

    companion object {
        /** 廃止済みレンジを除いた構成からルーターを作る。 */
        fun fromMetadata(metadata: Collection<RangeMetadata>): MultiRaftRouter {
            val current = metadata.filter { it.state != RangeState.DELETED }
            val ranges = current.map { it.keyRange }
            return MultiRaftRouter(
                RangeTable(ranges),
                ranges.associate { it.rangeId to GroupHandle(it.rangeId) },
                current.associate { it.keyRange.rangeId to it.epoch },
            )
        }
    }

    // 条件付き batch は全キーが同一グループに属する場合のみ受け付ける（跨グループは fail closed）。
    private fun routeBatch(command: RaftCommand.ConditionalBatch): GroupHandle {
        val keys = command.request.conditions.map { it.key } + command.request.mutations.map { it.key }
        val handles = keys.map { route(it) }
        val rangeIds = handles.map { it.rangeId }.toSet()
        if (rangeIds.size != 1) throw UnsupportedCrossGroupBatchException(rangeIds)
        return handles.first()
    }
}
