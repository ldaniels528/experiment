package com.qwery

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap
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
  val ROW_ID_BYTES = 4
  val SHORT_BYTES = 2

  // status bits
  val STATUS_BYTE = 1

  /**
   * Codec ByteBuffer
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CodecByteBuffer(val buf: ByteBuffer) extends AnyVal {

    @inline
    def getBigDecimal: java.math.BigDecimal = {
      val (scale, length) = (buf.getShort, buf.getShort)
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new java.math.BigDecimal(new BigInteger(bytes), scale)
    }

    @inline
    def getBigInteger: BigInteger = {
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new BigInteger(bytes)
    }

    @inline
    def getBlob: AnyRef = {
      val length = buf.getInt
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject()
    }

    @inline
    def putBlob(blob: AnyRef): ByteBuffer = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(blob)
      val bytes = baos.toByteArray
      buf.putInt(bytes.length).put(bytes)
    }

    @inline
    def getColumn: Column = {
      Column(name = buf.getString, metadata = buf.getColumnMetadata, maxSize = Some(buf.getInt))
    }

    @inline
    def putColumn(column: Column): ByteBuffer = {
      buf.putString(column.name).putShort(column.metadata.encode).putInt(column.maxLength - (SHORT_BYTES + STATUS_BYTE))
    }

    @inline
    def getColumnMetadata: ColumnMetadata = ColumnMetadata.decode(buf.getShort)

    @inline def getColumns: Seq[Column] = {
      val count = buf.getShort
      (0 until count).map(_ => Column.decode(buf))
    }

    @inline
    def putColumns(columns: Seq[Column]): ByteBuffer = {
      buf.putShort(columns.size.toShort)
      columns.map(_.encode).foreach(cb => buf.put(cb))
      buf
    }

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetadata: FieldMetadata = FieldMetadata.decode(buf.get)

    @inline def putFieldMetadata(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode)

    @inline def getRowID: ROWID = buf.getInt

    @inline def putRowID(rowID: ROWID): ByteBuffer = buf.putInt(rowID)

    @inline def getRowMetadata: RowMetadata = RowMetadata.decode(buf.get)

    @inline def putRowMetadata(rmd: RowMetadata): ByteBuffer = buf.put(rmd.encode)

    @inline
    def getString: String = {
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new String(bytes)
    }

    @inline
    def putString(string: String): ByteBuffer = buf.putShort(string.length.toShort).put(string.getBytes)

    @inline
    def getText(implicit fmd: FieldMetadata): String = {
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new String(bytes.decompressOrNah(fmd))
    }

    @inline
    def putText(string: String)(implicit fmd: FieldMetadata): ByteBuffer = {
      val bytes = string.getBytes.compressOrNah
      buf.putShort(bytes.length.toShort).put(bytes)
    }

    @inline
    def getUUID: UUID = new UUID(buf.getLong, buf.getLong)

    @inline
    def putUUID(uuid: UUID): ByteBuffer = buf.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)

  }

  /**
   * Math Utilities for Long integers
   * @param number the long integer
   */
  final implicit class MathUtilsLong(val number: Long) extends AnyVal {

    def toRowID: ROWID = number.toInt

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
