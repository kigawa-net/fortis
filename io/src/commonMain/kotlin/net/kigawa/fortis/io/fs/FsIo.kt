package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Input
import net.kigawa.fortis.io.Io
import net.kigawa.fortis.io.Output

data class FsIo(override val input: Input, override val output: Output): Io {
}