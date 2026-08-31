package net.kigawa.fortis.io.fs

import net.kigawa.fortis.io.Output

interface FsOutput: Output {
    val file: FortisFile
}