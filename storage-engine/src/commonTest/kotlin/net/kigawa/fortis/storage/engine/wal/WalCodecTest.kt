package net.kigawa.fortis.storage.engine.wal

import net.kigawa.fortis.storage.engine.wal.codec.WalCodec
import net.kigawa.fortis.storage.engine.wal.codec.WalDecodeResult
import net.kigawa.fortis.storage.engine.wal.codec.WalOperation
import net.kigawa.fortis.storage.engine.wal.codec.WalRecord
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class WalCodecTest {
    private val codec = WalCodec()

    @Test
    fun customCodecSettingsAreUsedForEncodingAndDecoding() {
        val customCodec = WalCodec(headerSize = 24, version = 2)
        val record = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2))

        val encoded = customCodec.encode(record)

        assertEquals(2.toByte(), encoded[4])
        assertEquals(26, encoded.size)
        assertContentEquals(byteArrayOf(1, 2), encoded.copyOfRange(24, 26))
        val result = assertIs<WalDecodeResult.Success>(customCodec.decode(encoded))
        assertEquals(record, result.record)
        assertEquals(encoded.size, result.bytesRead)
        assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun putRecordRoundTrips() {
        val record = WalRecord(
            sequence = 42L,
            operation = WalOperation.PUT,
            key = byteArrayOf(1, 2, 3),
            value = byteArrayOf(4, 5, 6, 7),
        )

        val encoded = codec.encode(record)
        val result = assertIs<WalDecodeResult.Success>(codec.decode(encoded))

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

        val result = assertIs<WalDecodeResult.Success>(codec.decode(codec.encode(record)))

        assertEquals(record, result.record)
        assertEquals(codec.headerSize + record.key.size, result.bytesRead)
    }

    @Test
    fun decodeCanReadRecordAtAnOffset() {
        val first = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2))
        val second = WalRecord(2L, WalOperation.DELETE, byteArrayOf(3), null)
        val prefix = byteArrayOf(99, 98)
        val data = prefix + codec.encode(first) + codec.encode(second)

        val firstResult = assertIs<WalDecodeResult.Success>(codec.decode(data, prefix.size))
        val secondOffset = prefix.size + firstResult.bytesRead
        val secondResult = assertIs<WalDecodeResult.Success>(codec.decode(data, secondOffset))

        assertEquals(first, firstResult.record)
        assertEquals(second, secondResult.record)
        assertEquals(data.size, secondOffset + secondResult.bytesRead)
    }

    @Test
    fun incompleteHeaderAndPayloadAreReportedAsIncomplete() {
        val encoded = codec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1, 2), byteArrayOf(3, 4)),
        )

        assertIs<WalDecodeResult.Incomplete>(codec.decode(encoded.copyOf(codec.headerSize - 1)))
        assertIs<WalDecodeResult.Incomplete>(codec.decode(encoded.copyOf(encoded.size - 1)))
    }

    @Test
    fun invalidOffsetAndMagicAreReportedAsCorrupted() {
        val encoded = codec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2)),
        )

        val invalidOffset = assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded, -1))
        assertEquals("Invalid offset: -1", invalidOffset.reason)

        val invalidMagic = encoded.copyOf()
        invalidMagic[0] = 'X'.code.toByte()
        val corrupted = assertIs<WalDecodeResult.Corrupted>(codec.decode(invalidMagic))
        assertEquals("Invalid WAL magic", corrupted.reason)
    }

    @Test
    fun invalidVersionIsReportedAsCorrupted() {
        val encoded = codec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2)),
        )
        encoded[4] = 127

        assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun invalidOperationCodeIsReportedAsCorrupted() {
        val encoded = codec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2)),
        )
        encoded[5] = 127

        assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun negativeKeyLengthIsReportedAsCorrupted() {
        val encoded = codec.encode(
            WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2)),
        )
        // keyLength = -1 in big-endian format.
        byteArrayOf(-1, -1, -1, -1).copyInto(encoded, destinationOffset = 14)

        assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded))
    }

    @Test
    fun valueLengthBelowMinusOneIsReportedAsCorrupted() {
        for (operation in WalOperation.entries) {
            val value = if (operation == WalOperation.PUT) byteArrayOf(2) else null
            val encoded = codec.encode(WalRecord(1L, operation, byteArrayOf(1), value))
            // valueLength = -2 in big-endian format.
            byteArrayOf(-1, -1, -1, -2).copyInto(encoded, destinationOffset = 18)

            assertIs<WalDecodeResult.Corrupted>(codec.decode(encoded), "$operation")
        }
    }

    @Test
    fun encoderRejectsInvalidOperationAndValueCombinations() {
        val putWithoutValue = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), null)
        val deleteWithValue = WalRecord(1L, WalOperation.DELETE, byteArrayOf(1), byteArrayOf(2))

        assertFailsWith<IllegalArgumentException> { codec.encode(putWithoutValue) }
        assertFailsWith<IllegalArgumentException> { codec.encode(deleteWithValue) }
    }

    @Test
    fun decoderDoesNotAliasEncodedPayload() {
        val record = WalRecord(1L, WalOperation.PUT, byteArrayOf(1), byteArrayOf(2))
        val encoded = codec.encode(record)
        val decoded = assertIs<WalDecodeResult.Success>(codec.decode(encoded)).record

        encoded[codec.headerSize] = 9
        encoded[codec.headerSize + record.key.size] = 8

        assertContentEquals(byteArrayOf(1), decoded.key)
        assertContentEquals(byteArrayOf(2), decoded.value)
    }
}
