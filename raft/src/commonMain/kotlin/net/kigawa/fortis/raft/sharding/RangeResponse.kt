package net.kigawa.fortis.raft.sharding

import net.kigawa.fortis.raft.transport.RaftPeerAddress

sealed interface RangeResponse<out T> {
    data class Success<T>(val value: T) : RangeResponse<T>
    data class LeaderUnavailable(val route: RangeRoute) : RangeResponse<Nothing>
}

/** 同じ経路と世代の要求を再送できる、既知のリーダーの接続先。 */
data class RedirectResponse(
    val route: RangeRoute,
    val leaderId: String,
    val leaderAddress: RaftPeerAddress,
) : RangeResponse<Nothing>

data class RangeScanSlice(val route: RangeRoute, val bounds: KeyRange) {
    init {
        require(bounds.rangeId == route.group.rangeId) { "scan bounds and route must identify the same range" }
    }
}
