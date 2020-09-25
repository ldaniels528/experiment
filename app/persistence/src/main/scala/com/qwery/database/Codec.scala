package com.qwery.database

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.util.UUID

import com.qwery.database.BlockDevice.Header
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

  def encode(column: Column, value_? : Option[Any]): Array[Byte] = {
    val fmd = FieldMetadata(column.metadata)
    encodeValue(fmd, value_?) match {
      case Some(fieldBuf) =>
        val bytes = fieldBuf.array()
        if (bytes.length > column.maxLength)
          throw new IllegalStateException(s"Column '${column.name}' is too long (${bytes.length} > ${column.maxLength})")
        allocate(STATUS_BYTE + bytes.length).putFieldMetadata(fmd).put(bytes).array()
      case None =>
        allocate(STATUS_BYTE).putFieldMetadata(fmd.copy(isNotNull = false)).array()
    }
  }

  def decodeColumns(buf: ByteBuffer): Seq[Column] = {
    val columnCount = buf.getShort
    for (_ <- 0 until columnCount) yield Column.decode(buf)
  }

  def encodeColumns(columns: Seq[Column]): ByteBuffer = {
    val columnBytes = columns.map(_.encode)
    val columnBytesLen = columnBytes.map(_.array().length).sum
    allocate(SHORT_BYTES + columnBytesLen)
      .putShort(columns.length.toShort).put(columnBytes.toArray.flatMap(_.array()))
  }

  def decodeObject(bytes: Array[Byte]): AnyRef = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    ois.readObject()
  }

  def encodeObject(item: AnyRef): Array[Byte] = {
    val baos = new ByteArrayOutputStream(8192)
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(item)
    oos.flush()
    baos.toByteArray
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
      buf.putString(column.name).putColumnMetadata(column.metadata).putInt(column.maxLength - (SHORT_BYTES + STATUS_BYTE))
    }

    @inline
    def getColumnMetadata: ColumnMetadata = ColumnMetadata.decode(buf.getShort)

    @inline
    def putColumnMetadata(metadata: ColumnMetadata): ByteBuffer = buf.putShort(metadata.encode)

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetadata: FieldMetadata = FieldMetadata.decode(buf.get)

    @inline def putFieldMetadata(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode)

    @inline def getHeader: BlockDevice.Header = BlockDevice.Header.fromBuffer(buf)

    @inline def putHeader(header: Header): ByteBuffer = buf.put(header.toBuffer)

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

}
