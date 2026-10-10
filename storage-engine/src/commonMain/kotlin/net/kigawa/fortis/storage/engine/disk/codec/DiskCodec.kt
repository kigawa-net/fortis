package net.kigawa.fortis.storage.engine.disk.codec

data class DiskCodec(
    val headerSize: Int = 14,
    val version: Byte = 1,
    val put: Byte = 1,
    val delete: Byte = 2,
) {
    val encoder = DiskCodecEncoder(headerSize,version,put,delete)
    val decoder = DiskCodecDecoder(headerSize,version,put,delete)
}
