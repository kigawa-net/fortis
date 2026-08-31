package net.kigawa.fortis.io.fs

import java.nio.file.Path

fun FsPath.toJvmPath(): Path = Path.of(toString())