package net.kigawa.fortis.raft

import net.kigawa.fortis.storage.engine.FortisStorageEngine

class StorageEngineStateMachine(
    private val storage: FortisStorageEngine,
): RaftStateMachine {
    override suspend fun apply(command: RaftCommand) = command.execute(storage)

    /** Local read; call through RaftRuntime.linearizableRead for a quorum-confirmed read. */
    suspend fun get(key: ByteArray): ByteArray? = storage.get(key)
}
