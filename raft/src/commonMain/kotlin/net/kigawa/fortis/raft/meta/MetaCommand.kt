package net.kigawa.fortis.raft.meta

import net.kigawa.fortis.raft.RaftCommand
import net.kigawa.fortis.raft.sharding.RangeId
import net.kigawa.fortis.raft.sharding.RangeMetadata

internal sealed interface MetaCommand {
    data class Bootstrap(val voterId: String) : MetaCommand
    data class AddLearner(val nodeId: String) : MetaCommand
    data class BeginPromotion(val nodeId: String) : MetaCommand
    data class FinishPromotion(val nodeId: String) : MetaCommand
    data class Register(val node: RegisteredNode) : MetaCommand
    data class UpdateNode(val node: RegisteredNode) : MetaCommand
    data class RemoveNode(val nodeId: String) : MetaCommand
    data class PutRange(val range: RangeMetadata) : MetaCommand
    data class Split(
        val parent: RangeId,
        val expectedEpoch: Long,
        val key: ByteArray,
        val left: RangeId,
        val right: RangeId,
    ) : MetaCommand
}

// The Meta group owns these keys. Put lets it reuse the log and RPC formats unchanged.
internal fun MetaCommand.toRaftCommand(): RaftCommand = RaftCommand.Put(
    ("fortis/meta/1/" + when (this) {
        is MetaCommand.Bootstrap -> "bootstrap"
        is MetaCommand.AddLearner -> "learner"
        is MetaCommand.BeginPromotion -> "begin-promotion"
        is MetaCommand.FinishPromotion -> "finish-promotion"
        is MetaCommand.Register -> "register"
        is MetaCommand.UpdateNode -> "update-node"
        is MetaCommand.RemoveNode -> "remove-node"
        is MetaCommand.PutRange -> "put-range"
        is MetaCommand.Split -> "split"
    }).encodeToByteArray(),
    MetaWriter().apply {
        when (val command = this@toRaftCommand) {
            is MetaCommand.Bootstrap -> string(command.voterId)
            is MetaCommand.AddLearner -> string(command.nodeId)
            is MetaCommand.BeginPromotion -> string(command.nodeId)
            is MetaCommand.FinishPromotion -> string(command.nodeId)
            is MetaCommand.Register -> node(command.node)
            is MetaCommand.UpdateNode -> node(command.node)
            is MetaCommand.RemoveNode -> string(command.nodeId)
            is MetaCommand.PutRange -> range(command.range)
            is MetaCommand.Split -> {
                string(command.parent.value)
                long(command.expectedEpoch)
                bytes(command.key)
                string(command.left.value)
                string(command.right.value)
            }
        }
    }.encode(),
)

internal fun RaftCommand.toMetaCommand(): MetaCommand {
    require(this is RaftCommand.Put) { "Invalid Meta Raft command" }
    return MetaReader(value).let { reader ->
        val command = with(reader) {
            when (key.decodeToString()) {
                "fortis/meta/1/bootstrap" -> MetaCommand.Bootstrap(string())
                "fortis/meta/1/learner" -> MetaCommand.AddLearner(string())
                "fortis/meta/1/begin-promotion" -> MetaCommand.BeginPromotion(string())
                "fortis/meta/1/finish-promotion" -> MetaCommand.FinishPromotion(string())
                "fortis/meta/1/register" -> MetaCommand.Register(node())
                "fortis/meta/1/update-node" -> MetaCommand.UpdateNode(node())
                "fortis/meta/1/remove-node" -> MetaCommand.RemoveNode(string())
                "fortis/meta/1/put-range" -> MetaCommand.PutRange(range())
                "fortis/meta/1/split" -> MetaCommand.Split(
                    RangeId(string()), long(), checkNotNull(bytes()), RangeId(string()), RangeId(string()),
                )
                else -> error("Unknown Meta Raft command")
            }
        }
        reader.requireEnd()
        command
    }
}

internal fun RaftCommand.isPromotion(): Boolean = this is RaftCommand.Put &&
    (key.contentEquals(beginPromotionKey) || key.contentEquals(finishPromotionKey))

private val beginPromotionKey = "fortis/meta/1/begin-promotion".encodeToByteArray()
private val finishPromotionKey = "fortis/meta/1/finish-promotion".encodeToByteArray()
