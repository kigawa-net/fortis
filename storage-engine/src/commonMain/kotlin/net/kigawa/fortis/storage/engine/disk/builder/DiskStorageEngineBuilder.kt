package net.kigawa.fortis.storage.engine.disk.builder

import net.kigawa.fortis.io.fs.FortisFile
import net.kigawa.fortis.io.fs.FsFileNotFoundException
import net.kigawa.fortis.io.fs.openRead
import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry
import net.kigawa.fortis.storage.engine.disk.engine.DiskStorageEngine
import net.kigawa.fortis.storage.engine.disk.codec.DiskCodec

data class DiskStorageEngineBuilder(
    val file: FortisFile?,
    val diskCodec: DiskCodec = DiskCodec(),
) {

    suspend fun build(): DiskStorageEngine {
        val file = requireNotNull(file) {
            "file is required"
        }

        val index =
            mutableMapOf<ByteArrayKey, DiskIndexEntry>()

        var endOffset = 0L

        try {
            file.openRead { input ->
                endOffset = DiskStorageIndexBuilder(
                    input = input,
                    diskCodec = diskCodec,
                ).buildIndex(index)
            }
        } catch (_: FsFileNotFoundException) {
            // 新規DBなので空状態で開始
        }

        return DiskStorageEngine(
            file = file,
            index = index,
            endOffset = endOffset,
            diskCodec = diskCodec
        )
    }

}