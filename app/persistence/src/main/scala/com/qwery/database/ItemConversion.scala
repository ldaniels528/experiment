package com.qwery.database

import java.lang.reflect.{Constructor, Field => JField}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.util.UUID

import com.qwery.database.ColumnTypes._
import com.qwery.database.Compression._
import com.qwery.database.FieldMetaData._idField
import com.qwery.database.ItemConversion._
import com.qwery.database.PersistentSeq.Field
import com.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.reflect.{ClassTag, classTag}

/**
 * Item Conversion of product/case classes
 */
abstract class ItemConversion[T <: Product : ClassTag] {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val constructors = TrieMap[Unit, (Constructor[_], Array[Class[_]])]()
  private val `class` = classTag[T].runtimeClass
  private val declaredFields: List[JField] = `class`.getDeclaredFields.toList
  private val declaredFieldNames: List[String] = declaredFields.map(_.getName)
  private val columns: List[Column] = toColumns(declaredFields)
  private val mappings: Map[String, Column] = Map(columns.map(c => c.name -> c): _*)

  // compute the column offsets
  private val columnOffsets: List[URID] = {
    case class Accum(agg: Int = 0, var last: Int = STATUS_BYTE, var list: List[Int] = Nil)
    columns.reverse.map(_.maxLength).foldLeft(Accum()) { (acc, maxLength) =>
      val index = acc.agg + acc.last
      acc.last = maxLength + index
      acc.list = index :: acc.list
      acc
    }.list.reverse
  }

  /**
   * Creates an item from a collection of fields
   * @param items the collection of [[Field fields]]
   * @return a new [[T item]]
   */
  def createItem(items: Seq[Field]): T = {
    val nameToValueMap = Map(items flatMap { case Field(name, _, value_?) => value_?.map(value => name -> value) }: _*)
    val rawValues = declaredFieldNames.map(nameToValueMap.get)
    val (constructor, parameterTypes) = constructors.getOrElseUpdate((), {
      val constructor = `class`.getConstructors.find(_.getParameterCount == rawValues.length)
        .getOrElse(throw new IllegalArgumentException(s"No suitable constructor found for ${`class`.getName}"))
      (constructor, constructor.getParameterTypes)
    })
    val normalizedValues = (parameterTypes zip rawValues).map {
      case (param, value) => if (param == classOf[Option[_]]) value else value.map(_.asInstanceOf[AnyRef]).orNull
    }
    constructor.newInstance(normalizedValues: _*).asInstanceOf[T]
  }

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = STATUS_BYTE + columns.map(_.maxLength).sum + 2 * LONG_BYTES

  private def toColumns(declaredFields: List[JField]): List[Column] = {
    val defaultMaxLen = 128
    declaredFields map { field =>
      val ci = Option(field.getDeclaredAnnotation(classOf[ColumnInfo]))
      val `type` = ColumnTypes.determineType(field.getType)
      val maxSize = ci.map(_.maxSize)
      if (`type`.getFixedLength.isEmpty && maxSize.isEmpty) {
        logger.warn(
          s"""|Column '${field.getName}' has no maximum value (default: $defaultMaxLen). Set one with the @ColumnInfo annotation:
              |
              |case class PixAllData(@(ColumnInfo@field)(maxSize = 36, isPrimary = true) idValue: String,
              |                      @(ColumnInfo@field)(maxSize = 10) idType: String,
              |                      responseTime: Int,
              |                      reportDate: Long,
              |                      _id: Long = 0L)
              |""".stripMargin)
      }
      Column(name = field.getName, `type` = `type`,
        maxSize = maxSize ?? Some(defaultMaxLen),
        isCompressed = ci.exists(_.isCompressed),
        isEncrypted = ci.exists(_.isEncrypted),
        isNullable = ci.exists(_.isNullable),
        isPrimary = ci.exists(_.isPrimary))
    } collect { case c if c.name != "_id" => c }
  }

  def toBytes(item: T): Array[Byte] = {
    val payloads = for {
      (name, value) <- toKeyValues(item)
      column <- mappings.get(name).toArray
    } yield encode(column, value)

    // convert the row to binary
    val buf = allocate(recordSize).putRowMetaData(RowMetaData())
    payloads.zipWithIndex foreach { case (bytes, index) =>
      buf.position(columnOffsets(index))
      buf.put(bytes)
    }
    buf.array()
  }

  def toBlocks(offset: URID, items: Seq[T]): Seq[(URID, ByteBuffer)] = {
    items.zipWithIndex.map { case (item, index) => (offset + index) -> wrap(toBytes(item)) }
  }

  def toBlocks(offset: URID, items: Traversable[T]): Stream[(URID, ByteBuffer)] = {
    items.toStream.zipWithIndex.map { case (item, index) => (offset + index) -> wrap(toBytes(item)) }
  }

  def toBytes(items: Seq[T]): Seq[ByteBuffer] = items.map(item => wrap(toBytes(item)))

  def toBytes(items: Traversable[T]): Stream[ByteBuffer] = items.map(item => wrap(toBytes(item))).toStream

  def toFields(buf: ByteBuffer): List[Field] = {
    columns.zipWithIndex.map { case (col, index) =>
      buf.position(columnOffsets(index))
      col.name -> decode(buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

  def toItem(id: URID, buf: ByteBuffer, evenDeletes: Boolean = false): Option[T] = {
    val metadata = buf.getRowMetaData
    if (metadata.isActive || evenDeletes) Some(createItem(items = _idField(id) :: toFields(buf))) else None
  }

  def toKeyValues(product: T): Seq[KeyValue] = declaredFieldNames zip product.productIterator.toSeq map {
    case (name, value: Option[_]) => name -> value
    case (name, value) => name -> Option(value)
  }

}

/**
 * ItemConversion Companion
 */
object ItemConversion extends Compression {

  def decode(buf: ByteBuffer): (FieldMetaData, Option[Any]) = {
    val fmd = buf.getFieldMetaData
    (fmd, decodeValue(fmd, buf))
  }

  def decodeValue(fmd: FieldMetaData, buf: ByteBuffer): Option[Any] = {
    if (fmd.isNull) None else {
      fmd.`type` match {
        case ArrayType => Some(buf.getArray)
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
        case StringType => Some(buf.getString(fmd))
        case UUIDType => Some(buf.getUUID)
        case unknown => throw new IllegalArgumentException(s"Unrecognized column type '$unknown'")
      }
    }
  }

  def encode(column: Column, value_? : Option[Any]): Array[Byte] = {
    val fmd = FieldMetaData(column)
    encodeValue(fmd, value_?) match {
      case Some(fieldBuf) =>
        val bytes = fieldBuf.array()
        if(bytes.length > column.maxLength)
          throw new IllegalStateException(s"Column '${column.name}' is too long (${bytes.length} > ${column.maxLength})")
        allocate(STATUS_BYTE + bytes.length).putFieldMetaData(fmd).put(bytes).array()
      case None =>
        allocate(STATUS_BYTE).putFieldMetaData(fmd.copy(isNotNull = false)).array()
    }
  }

  def encodeValue(fmd: FieldMetaData, value_? : Option[Any]): Option[ByteBuffer] = {
    value_? map {
      case a: Array[_] =>
        val bytes = a.flatMap(v => encodeValue(fmd, Option(v))).flatMap(_.array())
        allocate(INT_BYTES + bytes.length).putInt(bytes.length).put(bytes)
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
   * Codec ByteBuffer Extensions
   * @param buf the given [[ByteBuffer]]
   */
  final implicit class CodecByteBufferExtensions(val buf: ByteBuffer) extends AnyVal {

    @inline def getArray: Array[_] = ???

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetaData: FieldMetaData = FieldMetaData.decode(buf.get)

    @inline def putFieldMetaData(fmd: FieldMetaData): ByteBuffer = buf.put(fmd.encode.toByte)

    @inline def getRowMetaData: RowMetaData = RowMetaData.decode(buf.get)

    @inline def putRowMetaData(rmd: RowMetaData): ByteBuffer = buf.put(rmd.encode.toByte)

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

    def getString(implicit fmd: FieldMetaData): String = {
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

}
