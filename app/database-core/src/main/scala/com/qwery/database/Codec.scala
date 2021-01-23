package com.qwery.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.text.SimpleDateFormat
import java.util.UUID

import com.qwery.database.ColumnTypes._
import com.qwery.database.Compression.CompressionByteArrayExtensions
import com.qwery.database.types.{ArrayBlock, QxAny}

import scala.util.Try

/**
 * Codec Singleton
 */
object Codec extends Compression {

  def convertTo(value: Any, columnType: ColumnType): Any = {
    import ColumnTypes._

    val asNumber: Any => Number = {
      case value: Number => value
      case date: java.util.Date => date.getTime
      case x => Try(x.toString.toDouble: Number) // date string? (e.g. "Fri Sep 25 13:09:38 PDT 2020")
        .getOrElse(Try(new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy").parse(x.toString).getTime: Number)
          .getOrElse(throw TypeConversionException(x, columnType)))
    }

    columnType match {
      case BigIntType => asNumber(value) match {
        case b: BigInt => b
        case b: BigInteger => b
        case n => BigInt(n.longValue())
      }
      case BooleanType => value match {
        case b: Boolean => b
        case n: Number => n.doubleValue() != 0
        case x => throw TypeConversionException(x, columnType)
      }
      case ByteType => asNumber(value).byteValue()
      case CharType => value match {
        case c: Char => c
        case x =>
          val s = x.toString
          if (s.nonEmpty) s.charAt(0) else throw TypeConversionException(x, columnType)
      }
      case DateType => value match {
        case d: java.util.Date => d
        case n: Number => new java.util.Date(n.longValue())
        case x => new java.util.Date(asNumber(x).longValue())
      }
      case DoubleType => asNumber(value).doubleValue()
      case FloatType => asNumber(value).floatValue()
      case IntType => asNumber(value).intValue()
      case LongType => asNumber(value).longValue()
      case ShortType => asNumber(value).shortValue()
      case StringType => value.toString
      case UUIDType => value match {
        case u: UUID => u
        case x => Try(UUID.fromString(x.toString)).getOrElse(throw TypeConversionException(x, columnType))
      }
      case _ => value
    }
  }

  /**
   * Decodes the buffer as a value based on the given column
   * @param column the given [[Column column]]
   * @param buf    the [[ByteBuffer buffer]]
   * @return a tuple of [[FieldMetadata]] and the option of a value
   * @see [[QxAny.decode]]
   */
  def decode(column: Column, buf: ByteBuffer): (FieldMetadata, Option[Any]) = {
    implicit val fmd: FieldMetadata = buf.getFieldMetadata
    (fmd, decodeValue(column, buf))
  }

  /**
   * Encodes the given value into a byte array
   * @param column the given [[Column column]]
   * @param value  the option of a value
   * @return the byte array
   * @see [[QxAny.encode]]
   */
  def encode(column: Column, value: Option[Any]): Array[Byte] = {
    implicit val fmd: FieldMetadata = FieldMetadata(column.metadata)
    encodeValue(column, value) match {
      case Some(fieldBuf) =>
        val bytes = fieldBuf.array()
        assert(bytes.length <= column.maxPhysicalSize, throw ColumnCapacityExceededException(column, bytes.length))
        allocate(FieldMetadata.BYTES_LENGTH + bytes.length).putFieldMetadata(fmd).put(bytes).array()
      case None =>
        allocate(FieldMetadata.BYTES_LENGTH).putFieldMetadata(fmd.copy(isActive = false)).array()
    }
  }

  def decodeObject(bytes: Array[Byte]): AnyRef = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    ois.readObject()
  }

  def encodeObject(item: AnyRef): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(item)
    oos.flush()
    baos.toByteArray
  }

  def decodeArray(column: Column, buf: ByteBuffer)(implicit fmd: FieldMetadata): ArrayBlock = {
    // read the collection as a block
    val blockSize = buf.getInt
    val count = buf.getInt
    val bytes = new Array[Byte](blockSize)
    buf.get(bytes)

    // decode the items
    val block = wrap(bytes.decompressOrNah)
    ArrayBlock(`type` = block.getColumnMetadata.`type`, for {
      _ <- 0 until count
      (fmd, rawValue) = decode(column, block)
      value = if (fmd.isActive) QxAny(rawValue) else null
    } yield value)
  }

  def encodeArray(column: Column, array: ArrayBlock)(implicit fmd: FieldMetadata): ByteBuffer = {
    // create the data block
    val compressedBytes = (for {
      value_? <- array.items.toArray
      bytes <- value_?.encode(column)
    } yield bytes).compressOrNah

    // encode the items as a block
    val block = allocate(2 * INT_BYTES + compressedBytes.length)
    block.putInt(compressedBytes.length)
    block.putInt(array.length)
    block.put(compressedBytes)
    block
  }

  def decodeValue(column: Column, buf: ByteBuffer)(implicit fmd: FieldMetadata): Option[Any] = {
    if (fmd.isNull) None else {
      column.metadata.`type` match {
        case ArrayType => Some(decodeArray(column, buf))
        case BigDecimalType => Some(buf.getBigDecimal)
        case BigIntType => Some(buf.getBigInteger)
        case BinaryType => Some(buf.getBinary)
        case BlobType => Some(buf)
        case BooleanType => Some(buf.get > 0)
        case ByteType => Some(buf.get)
        case CharType => Some(buf.getChar)
        case ClobType => Some(buf)
        case DateType => Some(buf.getDate)
        case DoubleType => Some(buf.getDouble)
        case FloatType => Some(buf.getFloat)
        case IntType => Some(buf.getInt)
        case LongType => Some(buf.getLong)
        case ShortType => Some(buf.getShort)
        case StringType if column.isEnum => Some(column.enumValues(buf.getShort))
        case StringType => Some(buf.getText)
        case UUIDType => Some(buf.getUUID)
        case unknown => throw TypeConversionException(buf, unknown)
      }
    }
  }

  def encodeValue(column: Column, value_? : Option[Any])(implicit fmd: FieldMetadata): Option[ByteBuffer] = {
    value_?.map(convertTo(_, column.metadata.`type`)) map {
      case a: Array[_] if column.metadata.`type` == BinaryType => allocate(INT_BYTES + a.length).putBinary(a.asInstanceOf[Array[Byte]])
      case a: ArrayBlock => encodeArray(column, a)
      case b: BigDecimal => allocate(sizeOf(b)).putBigDecimal(b)
      case b: java.math.BigDecimal => allocate(sizeOf(b)).putBigDecimal(b)
      case b: java.math.BigInteger => allocate(sizeOf(b)).putBigInteger(b)
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
      case s: String if column.isEnum =>
        column.enumValues.indexOf(s) match {
          case -1 => die(s"'$s' is not allowed for column '${column.name}'")
          case index => allocate(SHORT_BYTES).putShort(index.toShort)
        }
      case s: String =>
        val bytes = s.getBytes.compressOrNah
        allocate(SHORT_BYTES + bytes.length).putShort(bytes.length.toShort).put(bytes)
      case u: UUID => allocate(LONG_BYTES * 2).putLong(u.getMostSignificantBits).putLong(u.getLeastSignificantBits)
      case v => throw TypeConversionException(v, column.metadata.`type`)
    }
  }

  def sizeOf(b: BigDecimal): Int = 2 * SHORT_BYTES + b.bigDecimal.unscaledValue().toByteArray.length

  def sizeOf(b: BigInt): Int = SHORT_BYTES + b.toByteArray.length

  /**
   * Codec ByteBuffer
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CodecByteBuffer(val buf: ByteBuffer) extends AnyVal {

    @inline
    def getBigDecimal: BigDecimal = {
      val scale = buf.getShort
      val length = buf.getShort
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new java.math.BigDecimal(new BigInteger(bytes), scale)
    }

    @inline
    def putBigDecimal(b: BigDecimal): ByteBuffer = {
      val bytes = b.bigDecimal.unscaledValue().toByteArray
      buf.putShort(b.scale.toShort).putShort(bytes.length.toShort).put(bytes)
    }

    @inline
    def getBigInteger: BigInteger = {
      val bytes = new Array[Byte](buf.getShort)
      buf.get(bytes)
      new BigInteger(bytes)
    }

    @inline
    def putBigInteger(b: BigInteger): ByteBuffer = {
      val array = b.toByteArray
      buf.putShort(array.length.toShort).put(array)
    }

    @inline
    def getBinary: Array[Byte] = {
      val length = buf.getInt
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      bytes
    }

    @inline
    def putBinary(bytes: Array[Byte]): ByteBuffer = buf.putInt(bytes.length).put(bytes)

    @inline
    def getBlob: Array[Byte] = {
      val length = buf.getInt
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      bytes
    }

    @inline
    def putBlob(bytes: Array[Byte]): ByteBuffer = {
      buf.putInt(bytes.length).put(bytes)
    }

    @inline
    def getClob(implicit fmd: FieldMetadata): CharSequence = {
      val length = buf.getInt
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new String(bytes.decompressOrNah)
    }

    @inline
    def putClob(chars: CharSequence)(implicit fmd: FieldMetadata): ByteBuffer = {
      val bytes = chars.toString.getBytes("utf-8").compressOrNah
      buf.putInt(bytes.length).put(bytes)
    }

    @inline
    def getColumn: Column = {
      Column(name = buf.getString, comment = buf.getString, metadata = buf.getColumnMetadata, enumValues = Nil, sizeInBytes = buf.getInt)
    }

    @inline
    def putColumn(column: Column): ByteBuffer = {
      buf.putString(column.name)
        .putString(column.comment)
        .putColumnMetadata(column.metadata)
        .putInt(column.sizeInBytes)
    }

    @inline
    def getColumnMetadata: ColumnMetadata = ColumnMetadata.decode(buf.getShort)

    @inline
    def putColumnMetadata(metadata: ColumnMetadata): ByteBuffer = buf.putShort(metadata.encode)

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetadata: FieldMetadata = FieldMetadata.decode(buf.get)

    @inline def putFieldMetadata(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode)

    @inline def getRowID: ROWID = buf.getInt

    @inline def putRowID(rowID: ROWID): ByteBuffer = buf.putInt(rowID)

    @inline def getRowMetadata: RowMetadata = RowMetadata.decode(buf.get)

    @inline def putRowMetadata(rmd: RowMetadata): ByteBuffer = buf.put(rmd.encode)

    @inline
    def getSerializableAs[T <: Serializable](implicit fmd: FieldMetadata): T = getSerializable.asInstanceOf[T]

    @inline
    def getSerializable(implicit fmd: FieldMetadata): Serializable = {
      val length = buf.getInt
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new ObjectInputStream(new ByteArrayInputStream(bytes.decompressOrNah)).readObject().asInstanceOf[Serializable]
    }

    @inline
    def putSerializable(value: Serializable)(implicit fmd: FieldMetadata): ByteBuffer = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(value)
      val bytes = baos.toByteArray.compressOrNah
      buf.putInt(bytes.length).put(bytes)
    }

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
      assert(length > 0, "Length must be positive")
      val bytes = new Array[Byte](length)
      buf.get(bytes)
      new String(bytes.decompressOrNah)
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

}
