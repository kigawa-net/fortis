package net.kigawa.fortis.raft.follower

import net.kigawa.fortis.raft.RaftPeerProgress
import net.kigawa.fortis.raft.append.AppendEntriesFactory
import net.kigawa.fortis.raft.append.AppendEntriesHandler
import net.kigawa.fortis.raft.append.AppendEntriesResponseHandler
import net.kigawa.fortis.raft.node.CommandAppender
import net.kigawa.fortis.raft.node.ElectionStarter
import net.kigawa.fortis.raft.node.RaftNode
import net.kigawa.fortis.raft.node.RaftTimer
import net.kigawa.fortis.raft.node.RequestVoteResponseHandler
import net.kigawa.fortis.raft.vote.RequestVoteHandler

class FollowerNode(
    peers: Map<String, RaftPeerProgress>, requestVoteHandler: RequestVoteHandler,
    appendEntriesHandler: AppendEntriesHandler,
    appendEntriesResponseHandler: AppendEntriesResponseHandler,
    appendEntriesFactory: AppendEntriesFactory, commandAppender: CommandAppender,
    electionStarter: ElectionStarter, requestVoteResponseHandler: RequestVoteResponseHandler,
    timer: RaftTimer,
): RaftNode(
    peers,
    requestVoteHandler, appendEntriesHandler, appendEntriesResponseHandler, appendEntriesFactory,
    commandAppender, electionStarter, requestVoteResponseHandler, timer
) {
}