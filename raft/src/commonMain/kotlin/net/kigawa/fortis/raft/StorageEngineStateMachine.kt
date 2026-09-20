package net.kigawa.fortis.raft

import net.kigawa.fortis.storage.engine.FortisStorageEngine

class StorageEngineStateMachine(
    private val storage: FortisStorageEngine,
) : RaftStateMachine {
    override suspend fun apply(command: RaftCommand) {
        when (command) {
            is RaftCommand.Put -> {
                storage.put(
                    key = command.key,
                    value = command.value,
                )
            }

            is RaftCommand.Delete -> {
                storage.delete(command.key)
            }
        }
    }
}