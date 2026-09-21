package net.kigawa.fortis.storage.engine.disk.codec

import net.kigawa.fortis.storage.engine.ByteArrayKey
import net.kigawa.fortis.storage.engine.disk.DiskIndexEntry

sealed interface DiskOperation {
    fun execute(
        index: MutableMap<ByteArrayKey, DiskIndexEntry>, storageKey: ByteArray, offset: Long, diskCodec: DiskCodec,
        header: DiskRecordHeader,
    )

    fun validateValueLength(valueLength: Int)

    data object Put: DiskOperation {
        override fun execute(
            index: MutableMap<ByteArrayKey, DiskIndexEntry>, storageKey: ByteArray, offset: Long, diskCodec: DiskCodec,
            header: DiskRecordHeader,
        ) {
            index[ByteArrayKey(storageKey)] =
                DiskIndexEntry(
                    valueOffset =
                        offset +
                            diskCodec.headerSize +
                            header.keyLength,
                    valueLength =
                        header.valueLength,
                )
        }

        override fun validateValueLength(valueLength: Int) {
            require(valueLength >= 0)
        }

    }

    data object Delete: DiskOperation {
        override fun execute(
            index: MutableMap<ByteArrayKey, DiskIndexEntry>, storageKey: ByteArray, offset: Long, diskCodec: DiskCodec,
            header: DiskRecordHeader,
        ) {

            index.remove(
                ByteArrayKey(storageKey)
            )
        }

        override fun validateValueLength(valueLength: Int) {
            require(valueLength == -1)
        }
    }
}
