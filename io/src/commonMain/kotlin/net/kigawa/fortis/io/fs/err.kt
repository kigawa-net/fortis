package net.kigawa.fortis.io.fs

open class FsException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)

class FsFileNotFoundException(
    val file: FortisFile,
    cause: Throwable? = null,
) : FsException(
    "File not found: ${file.path}",
    cause,
)