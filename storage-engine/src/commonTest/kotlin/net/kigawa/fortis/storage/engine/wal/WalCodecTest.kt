package net.kigawa.fortis.storage.engine.wal

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class WalCodecTest {
    @Test
    fun putRecordRoundTrips() {
        val record = WalRecord(
            sequence = 42L,
            operation = WalOperation.PUT,
            key = byteArrayOf(1, 2, 3),
            value = byteArrayOf(4, 5, 6, 7),
        )

        val encoded = WalCodec.encode(record)
        val result = assertIs<WalDecodeResult.Success>(WalCodec.decode(encoded))

        assertEquals(record, result.record)
        assertEquals(encoded.size, result.bytesRead)
    }

    @Test
    fun deleteRecordRoundTripsWithoutValue() {
        val record = WalRecord(
            sequence = Long.MIN_VALUE,
            operation = WalOperation.DELETE,
            key = byteArrayOf(9, 8),
            value = null,
        )

        val result = assertIs<WalDecodeResult.Success>(WalCodec.decode(WalCodec.encode(record)))

        assertEquals(record, result.record)
        assertEquals(WalCodec.HEADER_SIZE + record.key.size, result.bytesRead)
    }

    @Test
    fun decodeCanReadRecordAtAnOffset() {
        val first = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2))
        val second = WalRecord(2L, WalOperation.DELETE, byteArrayOf(3), null)
        val prefix = byteArrayOf(99, 98)
        val data = prefix + WalCodec.encode(first) + WalCodec.encode(second)

        val firstResult = assertIs<WalDecodeResult.Success>(WalCodec.decode(data, prefix.size))
        val secondOffset = prefix.size + firstResult.bytesRead
        val secondResult = assertIs<WalDecodeResult.Success>(WalCodec.decode(data, secondOffset))

        assertEquals(first, firstResult.record)
        assertEquals(second, secondResult.record)
        assertEquals(data.size, secondOffset + secondResult.bytesRead)
    }

    @Test
    fun incompleteHeaderAndPayloadAreReportedAsIncomplete() {
        val encoded = WalCodec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1, 2), byteArrayOf(3, 4)),
        )

        assertIs<WalDecodeResult.Incomplete>(WalCodec.decode(encoded.copyOf(WalCodec.HEADER_SIZE - 1)))
        assertIs<WalDecodeResult.Incomplete>(WalCodec.decode(encoded.copyOf(encoded.size - 1)))
    }

    @Test
    fun invalidOffsetAndMagicAreReportedAsCorrupted() {
        val encoded = WalCodec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2)),
        )

        val invalidOffset = assertIs<WalDecodeResult.Corrupted>(WalCodec.decode(encoded, -1))
        assertEquals("Invalid offset: -1", invalidOffset.reason)

        val invalidMagic = encoded.copyOf()
        invalidMagic[0] = 'X'.code.toByte()
        val corrupted = assertIs<WalDecodeResult.Corrupted>(WalCodec.decode(invalidMagic))
        assertEquals("Invalid WAL magic", corrupted.reason)
    }

    @Test
    fun encoderRejectsInvalidOperationAndValueCombinations() {
        val putWithoutValue = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), null)
        val deleteWithValue = WalRecord(1L, WalOperation.DELETE, byteArrayOf(1), byteArrayOf(2))

        assertFailsWith<IllegalArgumentException> { WalCodec.encode(putWithoutValue) }
        assertFailsWith<IllegalArgumentException> { WalCodec.encode(deleteWithValue) }
    }

    @Test
    fun decoderDoesNotAliasEncodedPayload() {
        val record = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2))
        val encoded = WalCodec.encode(record)
        val decoded = assertIs<WalDecodeResult.Success>(WalCodec.decode(encoded)).record

        encoded[WalCodec.HEADER_SIZE] = 9
        encoded[WalCodec.HEADER_SIZE + record.key.size] = 8

        assertContentEquals(byteArrayOf(1), decoded.key)
        assertContentEquals(byteArrayOf(2), decoded.value)
    }
}
