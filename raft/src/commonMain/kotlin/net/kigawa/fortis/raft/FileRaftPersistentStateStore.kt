package net.kigawa.fortis.raft

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsPath
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateCodec
import net.kigawa.fortis.raft.persistence.codec.RaftPersistentStateDecodeResult

/** One writer per state path. A successful save includes both file and directory durability. */
class FileRaftPersistentStateStore internal constructor(
    private val file: FortisFile,
    private val codec: RaftPersistentStateCodec,
    private val files: RaftStateFileAccess,
) : RaftPersistentStateStore {
    constructor(file: FortisFile, codec: RaftPersistentStateCodec = RaftPersistentStateCodec()) :
        this(file, codec, FortisRaftStateFileAccess)

    private val mutex = Mutex()
    private val name = (file.path.elements.lastOrNull() as? FsPath.Element.Name)?.name
        ?.also { require(it.isNotEmpty() && it != ".") { "State path must name a file" } }
        ?: error("State path must name a file")
    private val directory = file.path.copy(elements = file.path.elements.dropLast(1)).toFile()
    private val temporary = file.path.copy(
        elements = file.path.elements.dropLast(1) + FsPath.Element.Name("$name.tmp"),
    ).toFile()
    private var recovered = false
    private var publicationFailure: Throwable? = null

    override suspend fun load(): RaftPersistentState = mutex.withLock {
        ensureUsable()
        loadLocked()
    }

    override suspend fun save(term: Long, votedFor: String?) {
        val next = RaftPersistentState(term, votedFor)
        val data = codec.encode(next)
        mutex.withLock {
            ensureUsable()
            val previous = loadLocked()
            require(term >= previous.currentTerm) { "Persistent Raft term must not decrease" }
            require(term != previous.currentTerm || previous.votedFor == null || votedFor == previous.votedFor) {
                "A persistent Raft vote cannot be cleared or changed in the same term"
            }
            files.writeAndSync(temporary, data)
            publish()
        }
    }

    private suspend fun loadLocked(): RaftPersistentState {
        val canonical = files.read(file)
        if (canonical != null) {
            // A corrupt canonical file must never be replaced by a possibly older temporary state.
            val state = decode(canonical)
            if (!recovered) {
                files.syncFile(file)
                files.syncDirectory(directory)
                files.deleteIfExists(temporary)
                recovered = true
            }
            return state
        }
        val pending = files.read(temporary)
        if (pending == null) {
            recovered = true
            return RaftPersistentState()
        }
        // A complete first-save temporary state can be made durable; an incomplete one fails closed.
        val state = decode(pending)
        if (pending[4] != 2.toByte()) {
            throw RaftPersistentStateCorruptionException("Temporary Raft state requires a verified checksum")
        }
        files.syncFile(temporary)
        publish()
        recovered = true
        return state
    }

    private suspend fun publish() {
        // Cancellation must not leave the rename without its required directory sync.
        withContext(NonCancellable) {
            try {
                files.atomicReplace(temporary, file)
                files.syncDirectory(directory)
            } catch (cause: Throwable) {
                // Rename may have occurred even if an I/O operation reports failure.
                publicationFailure = cause
                throw cause
            }
        }
    }

    private fun ensureUsable() {
        publicationFailure?.let {
            throw RaftPersistentStatePersistenceException(
                "Raft state publication failed; reload with a new store before proceeding", it,
            )
        }
    }

    private fun decode(data: ByteArray): RaftPersistentState = when (val result = codec.decode(data)) {
        is RaftPersistentStateDecodeResult.Success -> result.state
        RaftPersistentStateDecodeResult.Incomplete -> throw RaftPersistentStateCorruptionException(
            "Incomplete Raft persistent state",
        )
        is RaftPersistentStateDecodeResult.Corrupted -> throw RaftPersistentStateCorruptionException(result.reason)
    }
}
