package net.kigawa.fortis.storage.engine.disk

class DiskCorruptionException(
    message: String, cause: Throwable? = null
) : Exception(message, cause)