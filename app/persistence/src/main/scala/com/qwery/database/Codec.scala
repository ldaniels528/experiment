package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.util.UUID

import com.qwery.database.ColumnTypes._
import com.qwery.database.Compression.CompressionByteArrayExtensions

/**
 * Codec Singleton
 */
object Codec extends Compression {

  def decode(buf: ByteBuffer): (FieldMetadata, Option[Any]) = {
    val fmd = buf.getFieldMetadata
    (fmd, decodeValue(fmd, buf))
  }

  def decodeSeq(buf: ByteBuffer)(implicit fmd: FieldMetadata): Seq[Option[Any]] = {
    // read the collection as a block
    val blockSize = buf.getInt
    val count = buf.getInt
    val bytes = new Array[Byte](blockSize)
    buf.get(bytes)

    // decode the items
    val block = wrap(bytes.decompressOrNah)
    for {
      _ <- 0 to count
      (_, value) = decode(block)
    } yield value
  }

  def decodeValue(fmd: FieldMetadata, buf: ByteBuffer): Option[Any] = {
    if (fmd.isNull) None else {
      fmd.`type` match {
        case ArrayType => Some(decodeSeq(buf)(fmd))
        case BigDecimalType => Some(buf.getBigDecimal)
        case BigIntType => Some(buf.getBigInteger)
        case BlobType => Some(buf)
        case BooleanType => Some(buf.get > 0)
        case ByteType => Some(buf.get)
        case CharType => Some(buf.getChar)
        case DateType => Some(buf.getDate)
        case DoubleType => Some(buf.getDouble)
        case FloatType => Some(buf.getFloat)
        case IntType => Some(buf.getInt)
        case LongType => Some(buf.getLong)
        case ShortType => Some(buf.getShort)
        case StringType => Some(buf.getText(fmd))
        case UUIDType => Some(buf.getUUID)
        case unknown => throw new IllegalArgumentException(s"Unrecognized column type '$unknown'")
      }
    }
  }

  def encode(column: Column, value_? : Option[Any]): Array[Byte] = {
    val fmd = FieldMetadata(column.metadata)
    encodeValue(fmd, value_?) match {
      case Some(fieldBuf) =>
        val bytes = fieldBuf.array()
        if(bytes.length > column.maxLength)
          throw new IllegalStateException(s"Column '${column.name}' is too long (${bytes.length} > ${column.maxLength})")
        allocate(STATUS_BYTE + bytes.length).putFieldMetadata(fmd).put(bytes).array()
      case None =>
        allocate(STATUS_BYTE).putFieldMetadata(fmd.copy(isNotNull = false)).array()
    }
  }

  def encodeSeq(items: Seq[Any])(implicit fmd: FieldMetadata): ByteBuffer = {
    // create the data block
    val compressedBytes = (for {
      value_? <- items.toArray map {
        case o: Option[_] => o
        case v => Option(v)
      }
      buf <- encodeValue(fmd, value_?).toArray
      bytes <- buf.array()
    } yield bytes).compressOrNah

    // encode the items as a block
    val block = allocate(2 * INT_BYTES + compressedBytes.length)
    block.putInt(compressedBytes.length)
    block.putInt(items.length)
    block.put(compressedBytes)
    block
  }

  def encodeValue(fmd: FieldMetadata, value_? : Option[Any]): Option[ByteBuffer] = {
    value_? map {
      case a: Array[_] => encodeSeq(a)(fmd)
      case b: java.math.BigDecimal =>
        val bytes = b.unscaledValue().toByteArray
        allocate(SHORT_BYTES * 2 + bytes.length).putShort(b.scale.toShort).putShort(bytes.length.toShort).put(bytes)
      case b: java.math.BigInteger =>
        val bytes = b.toByteArray
        allocate(SHORT_BYTES + bytes.length).putShort(bytes.length.toShort).put(bytes)
      case b: Boolean => allocate(ONE_BYTE).put((if (b) 0 else 1).toByte)
      case b: Byte => allocate(ONE_BYTE).put(b)
      case b: ByteBuffer => b
      case c: Char => allocate(SHORT_BYTES).putChar(c)
      case d: java.util.Date => allocate(LONG_BYTES).putDate(d)
      case d: Double => allocate(LONG_BYTES).putDouble(d)
      case f: Float => allocate(INT_BYTES).putFloat(f)
      case i: Int => allocate(INT_BYTES).putInt(i)
      case l: Long => allocate(LONG_BYTES).putLong(l)
      case s: Short => allocate(SHORT_BYTES).putShort(s)
      case s: String =>
        val bytes = s.getBytes.compressOrNah(fmd)
        allocate(SHORT_BYTES + bytes.length).putShort(bytes.length.toShort).put(bytes)
      case u: UUID => allocate(LONG_BYTES * 2).putLong(u.getMostSignificantBits).putLong(u.getLeastSignificantBits)
      case v => throw new IllegalArgumentException(s"Unrecognized type '${v.getClass.getSimpleName}' ($v)")
    }
  }

}
