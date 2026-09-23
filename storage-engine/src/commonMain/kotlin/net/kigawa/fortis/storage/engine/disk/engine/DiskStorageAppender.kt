package net.kigawa.fortis.storage.engine.disk.engine

import net.kigawa.fortis.io.fs.FortisFile
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
            var writtenTotal = 0

            while (writtenTotal < data.size) {
                val written = output.writeAt(
                    offset = offset + writtenTotal,
                    data = data,
                    dataOffset = writtenTotal,
                    length = data.size - writtenTotal,
                )

                check(written > 0) {
                    "Disk write made no progress"
                }

                writtenTotal += written
            }

            output.sync()
        }

        endOffset += data.size

        return offset
    }
}