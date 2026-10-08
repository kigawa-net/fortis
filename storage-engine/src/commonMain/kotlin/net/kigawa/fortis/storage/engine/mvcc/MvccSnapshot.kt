package net.kigawa.fortis.storage.engine.mvcc

// 読み取り時点を表すスナップショット
data class MvccSnapshot(
    val version: Long,
)
