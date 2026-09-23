package net.kigawa.fortis.storage.engine.disk.codec

data class DiskRecordHeader(
    val operation: DiskOperation,
    val keyLength: Int,
    val valueLength: Int,
)