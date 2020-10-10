package com.qwery.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.qwery.database.ColumnTypes._
import com.qwery.database.Compression.CompressionByteArrayExtensions

import scala.util.Try

/**
 * Codec Singleton
 */
object Codec extends Compression {

  def convertTo(value: Any, columnType: ColumnType): Any = {
    import ColumnTypes._

    val asNumber: Any => Number = {
      case value: Number => value
      case date: Date => date.getTime
      case x => Try(x.toString.toDouble: Number) // date string? (e.g. "Fri Sep 25 13:09:38 PDT 2020")
        .getOrElse(Try(new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy").parse(x.toString).getTime: Number)
          .getOrElse(throw new IllegalArgumentException(s"'$x' is not a Numeric value")))
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
        case x => throw new IllegalArgumentException(s"'$x' is not a Boolean value")
      }
      case ByteType => asNumber(value).byteValue()
      case DateType => value match {
        case d: java.util.Date => d
        case n: Number => new java.util.Date(n.longValue())
        case x => new java.util.Date(asNumber(x).longValue())
      }
      case CharType => value match {
        case c: Char => c
        case x =>
          val s = x.toString
          if (s.nonEmpty) s.charAt(0) else throw new IllegalArgumentException(s"'$x' is not a Character value")
      }
      case DoubleType => asNumber(value).doubleValue()
      case FloatType => asNumber(value).floatValue()
      case IntType => asNumber(value).intValue()
      case LongType => asNumber(value).longValue()
      case ShortType => asNumber(value).shortValue()
      case StringType => value.toString
      case UUIDType => value match {
        case u: UUID => u
        case x => Try(UUID.fromString(x.toString))
          .getOrElse(throw new IllegalArgumentException(s"'$x' is not a UUID value"))
      }
      case _ => value
    }
  }

  def decode(column: Column, buf: ByteBuffer): (FieldMetadata, Option[Any]) = {
    implicit val fmd: FieldMetadata = buf.getFieldMetadata
    (fmd, decodeValue(column, buf))
  }

  def encode(column: Column, value_? : Option[Any]): Array[Byte] = {
    implicit val fmd: FieldMetadata = FieldMetadata(column.metadata)
    encodeValue(column, value_?) match {
      case Some(fieldBuf) =>
        val bytes = fieldBuf.array()
        assert(bytes.length <= column.maxPhysicalSize, s"Column '${column.name}' is too long (${bytes.length} > ${column.maxPhysicalSize})")
        allocate(STATUS_BYTE + bytes.length).putFieldMetadata(fmd).put(bytes).array()
      case None =>
        allocate(STATUS_BYTE).putFieldMetadata(fmd.copy(isActive = false)).array()
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

  def decodeSeq(column: Column, buf: ByteBuffer)(implicit fmd: FieldMetadata): Seq[Option[Any]] = {
    // read the collection as a block
    val blockSize = buf.getInt
    val count = buf.getInt
    val bytes = new Array[Byte](blockSize)
    buf.get(bytes)

    // decode the items
    val block = wrap(bytes.decompressOrNah)
    for {
      _ <- 0 until count
      (_, value) = decode(column, block)
    } yield value
  }

  def encodeSeq(column: Column, items: Seq[Any])(implicit fmd: FieldMetadata): ByteBuffer = {
    // create the data block
    val compressedBytes = (for {
      value_? <- items.toArray map {
        case o: Option[_] => o
        case v => Option(v)
      }
      buf <- encodeValue(column, value_?).toArray
      bytes <- buf.array()
    } yield bytes).compressOrNah

    // encode the items as a block
    val block = allocate(2 * INT_BYTES + compressedBytes.length)
    block.putInt(compressedBytes.length)
    block.putInt(items.length)
    block.put(compressedBytes)
    block
  }

  def decodeValue(column: Column, buf: ByteBuffer)(implicit fmd: FieldMetadata): Option[Any] = {
    if (fmd.isNull) None else {
      column.metadata.`type` match {
        case ArrayType => Some(decodeSeq(column, buf))
        case BigDecimalType => Some(buf.getBigDecimal)
        case BigIntType => Some(buf.getBigInteger)
        case BinaryType => Some(buf.getBinary)
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
        case StringType => Some(buf.getText)
        case UUIDType => Some(buf.getUUID)
        case unknown => throw new IllegalArgumentException(s"Unrecognized column type '$unknown'")
      }
    }
  }

  def encodeValue(column: Column, value_? : Option[Any])(implicit fmd: FieldMetadata): Option[ByteBuffer] = {
    value_?.map(convertTo(_, column.metadata.`type`)) map {
      case a: Array[_] if column.metadata.`type` == BinaryType => allocate(INT_BYTES + a.length).putBinary(a.asInstanceOf[Array[Byte]])
      case a: Array[_] => encodeSeq(column, a)
      case b: BigDecimal => allocate(sizeOf(b)).putBigDecimal(b)
      case b: java.math.BigDecimal => allocate(sizeOf(b)).putBigDecimal(b)
      case b: java.math.BigInteger => allocate(LONG_BYTES).putBigInteger(b)
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
        val bytes = s.getBytes.compressOrNah
        allocate(SHORT_BYTES + bytes.length).putShort(bytes.length.toShort).put(bytes)
      case u: UUID => allocate(LONG_BYTES * 2).putLong(u.getMostSignificantBits).putLong(u.getLeastSignificantBits)
      case v => throw new IllegalArgumentException(s"Unrecognized type '${v.getClass.getSimpleName}' ($v)")
    }
  }

  def sizeOf(b: BigDecimal): Int = {
    val size = 2 * SHORT_BYTES + b.bigDecimal.unscaledValue().toByteArray.length
    size
  }

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
      val bytes = new Array[Byte](LONG_BYTES)
      buf.get(bytes)
      new BigInteger(bytes)
    }

    @inline
    def putBigInteger(b: BigInteger): ByteBuffer = buf.put(b.toByteArray)

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
      Column(name = buf.getString, comment = buf.getString, metadata = buf.getColumnMetadata, sizeInBytes = buf.getInt)
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
