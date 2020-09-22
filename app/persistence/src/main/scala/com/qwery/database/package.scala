package com.qwery

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.UUID

import com.qwery.database.Compression.CompressionByteArrayExtensions

/**
 * Qwery database package object
 */
package object database {
  type Block = (ROWID, ByteBuffer)
  type KeyValue = (String, Option[Any])
  type ROWID = Int

  // byte quantities
  val ONE_BYTE = 1
  val INT_BYTES = 4
  val LONG_BYTES = 8
  val SHORT_BYTES = 2

  // status bits
  val STATUS_BYTE = 1

  /**
   * Codec ByteBuffer Extensions
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CodecByteBufferExtensions(val buf: ByteBuffer) extends AnyVal {

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetaData: FieldMetadata = FieldMetadata.decode(buf.get)

    @inline def putFieldMetaData(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode.toByte)

    @inline def getRowMetaData: RowMetadata = RowMetadata.decode(buf.get)

    @inline def putRowMetaData(rmd: RowMetadata): ByteBuffer = buf.put(rmd.encode.toByte)

    def getBigDecimal: java.math.BigDecimal = {
      val (scale, length) = (buf.getShort, buf.getShort)
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new java.math.BigDecimal(new BigInteger(bytes), scale)
    }

    def getBigInteger: BigInteger = {
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new BigInteger(bytes)
    }

    def getString(implicit fmd: FieldMetadata): String = {
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new String(bytes.decompressOrNah(fmd))
    }

    def getUUID: UUID = {
      val bytes = new Array[Byte](16)
      buf.get(bytes)
      UUID.nameUUIDFromBytes(bytes)
    }

  }

  /**
   * Math Utilities for Long integers
   * @param number the long integer
   */
  final implicit class MathUtilsLong(val number: Long) extends AnyVal {

    def toURID: ROWID = number.toInt

    def isPrime: Boolean = number match {
      case n if n < 2 => false
      case 2 => true
      case n =>
        var m: Long = 1L
        while (m < n / m) {
          m += 1
          if (n % m == 0) return false
        }
        true
    }

  }

}
