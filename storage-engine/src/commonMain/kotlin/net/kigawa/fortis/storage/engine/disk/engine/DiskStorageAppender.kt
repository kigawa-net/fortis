package net.kigawa.fortis.storage.engine.disk.engine

import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsOutput
import net.kigawa.fortis.io.fs.openWrite

class DiskStorageAppender(
    var endOffset: Long,
    val file: FortisFile,
) {
    suspend fun append(
        data: ByteArray,
    ): Long {
        val offset = endOffset
        file.openWrite(isCreate = true) { output ->
            write(output, data, offset)
        }
        endOffset += data.size
        return offset
    }

    private suspend fun write(output: FsOutput, data: ByteArray, offset: Long) {
        var writtenTotal = 0
        while (writtenTotal < data.size) {
            writtenTotal += writeNext(output, offset, writtenTotal, data)
        }
        output.sync()
    }

    private suspend fun writeNext(output: FsOutput, offset: Long, writtenTotal: Int, data: ByteArray): Int {
        val written = output.writeAt(
            offset = offset + writtenTotal,
            data = data,
            dataOffset = writtenTotal,
            length = data.size - writtenTotal,
        )

        check(written > 0) {
            "Disk write made no progress"
        }
        return written
    }
}