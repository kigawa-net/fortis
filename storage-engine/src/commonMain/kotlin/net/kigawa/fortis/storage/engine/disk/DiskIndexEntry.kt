package net.kigawa.fortis.storage.engine.disk
data class DiskIndexEntry(
    val valueOffset: Long,
    val valueLength: Int,
)