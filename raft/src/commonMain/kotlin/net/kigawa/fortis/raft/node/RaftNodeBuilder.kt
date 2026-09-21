package net.kigawa.fortis.raft.node

import net.kigawa.fortis.raft.*
import net.kigawa.fortis.raft.append.AppendEntriesFactory
import net.kigawa.fortis.raft.append.AppendEntriesHandler
import net.kigawa.fortis.raft.append.AppendEntriesResponseHandler
import net.kigawa.fortis.raft.log.RaftLog
import net.kigawa.fortis.raft.vote.RaftVolatileState
import net.kigawa.fortis.raft.vote.RequestVoteHandler

data class RaftNodeBuilder(
    val nodeId: String,
    val peers: MutableMap<String, RaftPeerProgress>,
    val persistentState: RaftPersistentState,
    val volatileState: RaftVolatileState,
    val log: RaftLog,
    val stateMachine: RaftStateMachine,
    val applier: RaftApplier =
        RaftApplier(
            volatileState = volatileState,
            log = log,
            stateMachine = stateMachine,
        ),
    val requestVoteHandler: RequestVoteHandler =
        RequestVoteHandler(
            state = persistentState,
            log = log,
            persistentState = persistentState,
        ),
    private val appendEntriesHandler: AppendEntriesHandler =
        AppendEntriesHandler(
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
            applier = applier,
        ),
    private val commitAdvancer: RaftCommitAdvancer =
        RaftCommitAdvancer(
            persistentState = persistentState,
            volatileState = volatileState,
            log = log,
        ),
    private val appendEntriesResponseHandler: AppendEntriesResponseHandler =
        AppendEntriesResponseHandler(
            persistentState = persistentState,
            commitAdvancer = commitAdvancer,
            applier = applier,
        ),
    private val appendEntriesFactory: AppendEntriesFactory =
        AppendEntriesFactory(
            nodeId = nodeId,
            state = persistentState,
            volatileState = volatileState,
            log = log,
        ),
    private val commandAppender: CommandAppender =
        CommandAppender(
            persistentState = persistentState,
            log = log,
            commitAdvancer = commitAdvancer,
            applier = applier,
        ),
    private val electionStarter: ElectionStarter =
        ElectionStarter(
            persistentState = persistentState,
            nodeId = nodeId,
            log = log,
        ),
    private val requestVoteResponseHandler: RequestVoteResponseHandler =
        RequestVoteResponseHandler(
            persistentState = persistentState,
            electionStarter = electionStarter,
        ),
) {
    fun build(): RaftNode = RaftNode(
        peers,
        requestVoteHandler,
        appendEntriesHandler,
        appendEntriesResponseHandler,
        appendEntriesFactory,
        commandAppender,
        electionStarter,
        requestVoteResponseHandler,
    )
}
