package net.kigawa.fortis.storage.engine.wal

enum class WalOperation(
    val code: Byte,
) {
    PUT(1),
    DELETE(2);

    companion object {
        fun fromCode(code: Byte): WalOperation? =
            entries.firstOrNull { it.code == code }
    }
}