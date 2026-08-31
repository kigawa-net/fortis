package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Io

data class FsIo(override val input: FsInput, override val output: FsOutput): Io {
}