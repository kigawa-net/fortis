package net.kigawa.fortis.raft.snapshot

import net.kigawa.fortis.io.fs.*
import net.kigawa.fortis.raft.FortisRaftStateFileAccess
import net.kigawa.fortis.raft.RaftStateFileAccess

/** 同一ディレクトリの一時ファイルから永続データを公開する。 */
internal suspend fun replaceSnapshotFile(
    file: FortisFile,
    data: ByteArray,
    access: RaftStateFileAccess = FortisRaftStateFileAccess,
) {
    val parent = file.path.copy(elements = file.path.elements.dropLast(1))
    val temporary = parent.copy(elements = parent.elements +
        FsPath.Element.Name(file.path.elements.last().toString() + ".snapshot-tmp")).toFile()
    access.writeAndSync(temporary, data)
    access.atomicReplace(temporary, file)
    access.syncDirectory(parent.toFile())
}

/** 境界と本文の破損検出に使うチェックサム。 */
internal fun snapshotChecksum(bytes: ByteArray, length: Int): Int {
    var hash = 0x811c9dc5.toInt()
    for (index in 0 until length) hash = (hash xor (bytes[index].toInt() and 255)) * 16777619
    return hash
}

/** 再起動直前に公開された世代も、利用前に永続化を確定する。 */
internal suspend fun syncSnapshotFile(file: FortisFile) {
    FortisRaftStateFileAccess.syncFile(file)
    FortisRaftStateFileAccess.syncDirectory(file.path.copy(elements = file.path.elements.dropLast(1)).toFile())
}
