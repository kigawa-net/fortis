package net.kigawa.fortis.io.fs

data class FortisFile(val path: FsPath) {
}

expect suspend fun FortisFile.openRead(block: suspend (input: FsInput) -> Unit)

expect suspend fun FortisFile.openWrite(isCreate: Boolean, block: suspend (output: FsOutput) -> Unit)

expect suspend fun FortisFile.openReadWrite(isCreate: Boolean, block: suspend (io: FsIo) -> Unit)

/** Replaces [target] atomically on the same filesystem; never falls back to copy/delete. */
expect suspend fun FortisFile.atomicReplace(target: FortisFile)

/** Persists directory entry changes. Throws when the filesystem cannot provide this operation. */
expect suspend fun FortisFile.syncDirectory()

expect suspend fun FortisFile.deleteIfExists()
