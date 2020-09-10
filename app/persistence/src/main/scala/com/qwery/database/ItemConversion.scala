package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.util.UUID

import com.qwery.database.ColumnTypes._
import com.qwery.database.Compression._
import com.qwery.database.ItemConversion._
import com.qwery.database.PersistentSeq.Field
import com.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.reflect.{ClassTag, classTag}

/**
 * Item Conversion of product/case classes
 */
abstract class ItemConversion[T <: Product : ClassTag] {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  // cache the class information for type T
  private val `class` = classTag[T].runtimeClass
  private val declaredFields = `class`.getDeclaredFields.toList
  private val declaredFieldNames = declaredFields.map(_.getName)
  private val constructor = `class`.getConstructors.find(_.getParameterCount == declaredFields.length)
    .getOrElse(throw new IllegalArgumentException(s"No suitable constructor found for class ${`class`.getName}"))
  private val parameterTypes = constructor.getParameterTypes

  // determine the columns for type T
  protected val columns: List[Column] = toColumns
  private val physicalColumns: List[Column] = columns.filterNot(_.isRowID)
  private val nameToColumnMap: Map[String, Column] = Map(columns.map(c => c.name -> c): _*)

  // define a closure to dynamically create the optional rowID field for type T
  private val toRowIdField: ROWID => Option[Field] = {
    val rowIdColumn_? = columns.find(_.isRowID)
    val fmd = FieldMetaData(isCompressed = false, isEncrypted = false, isNotNull = false, `type` = ColumnTypes.LongType)
    (rowID: ROWID) => rowIdColumn_?.map(c => Field(name = c.name, fmd, value = Some(rowID)))
  }

  // compute the column offsets for type T
  protected val columnOffsets: List[ROWID] = {
    case class Accumulator(agg: Int = 0, var last: Int = STATUS_BYTE, var list: List[Int] = Nil)
    physicalColumns.reverse.map(_.maxLength).foldLeft(Accumulator()) { (acc, maxLength) =>
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
    val nameToValueMap: Map[String, Option[Any]] = Map(items.collect { case f if f.value.nonEmpty => f.name -> f.value }: _*)
    val rawValues = declaredFieldNames.flatMap(nameToValueMap.get)
    val normalizedValues = (parameterTypes zip rawValues) map { case (param, value) =>
      if (param == classOf[Option[_]]) value else value.map(_.asInstanceOf[AnyRef]).orNull
    }
    constructor.newInstance(normalizedValues: _*).asInstanceOf[T]
  }

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = STATUS_BYTE + physicalColumns.map(_.maxLength).sum + 2 * LONG_BYTES

  def toBlocks(offset: ROWID, items: Seq[T]): Seq[(ROWID, ByteBuffer)] = {
    items.zipWithIndex.map { case (item, index) => (offset + index) -> wrap(toBytes(item)) }
  }

  def toBlocks(offset: ROWID, items: Traversable[T]): Stream[(ROWID, ByteBuffer)] = {
    items.toStream.zipWithIndex.map { case (item, index) => (offset + index) -> wrap(toBytes(item)) }
  }

  def toBytes(item: T): Array[Byte] = {
    val payloads = for {
      (name, value_?) <- toKeyValues(item)
      column <- nameToColumnMap.get(name).toArray if !column.isRowID
    } yield encode(column, value_?)

    // convert the row to binary
    val buf = allocate(recordSize).putRowMetaData(RowMetaData())
    payloads.zipWithIndex foreach { case (bytes, index) =>
      buf.position(columnOffsets(index))
      buf.put(bytes)
    }
    buf.array()
  }

  def toBytes(items: Seq[T]): Seq[ByteBuffer] = items.map(item => wrap(toBytes(item)))

  def toBytes(items: Traversable[T]): Stream[ByteBuffer] = items.map(item => wrap(toBytes(item))).toStream

  def toColumns: List[Column] = {
    val defaultMaxLen = 128
    declaredFields map { field =>
      val ci = Option(field.getDeclaredAnnotation(classOf[ColumnInfo]))
      val `type` = ColumnTypes.determineType(field.getType)
      val maxSize = ci.map(_.maxSize)
      if (`type`.getFixedLength.isEmpty && maxSize.isEmpty) {
        logger.warn(
          s"""|Column '${field.getName}' has no maximum value (default: $defaultMaxLen). Set one with the @ColumnInfo annotation:
              |
              |case class StockQuote(@(ColumnInfo@field)(maxSize = 36)   idValue: String,
              |                      @(ColumnInfo@field)(maxSize = 10)   idType: String,
              |                                                          responseTime: Int,
              |                                                          reportDate: Long,
              |                      @(ColumnInfo@field)(isRowID = true) rowID: ROWID)
              |""".stripMargin)
      }
      Column(name = field.getName, `type` = `type`,
        maxSize = maxSize ?? Some(defaultMaxLen),
        isCompressed = ci.exists(_.isCompressed),
        isEncrypted = ci.exists(_.isEncrypted),
        isNullable = ci.exists(_.isNullable),
        isPrimary = ci.exists(_.isPrimary),
        isRowID = ci.exists(_.isRowID))
    }
  }

  def toFields(buf: ByteBuffer): List[Field] = {
    physicalColumns.zipWithIndex map { case (col, index) =>
      buf.position(columnOffsets(index))
      col.name -> decode(buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

  def toItem(id: ROWID, buf: ByteBuffer, evenDeletes: Boolean = false): Option[T] = {
    val metadata = buf.getRowMetaData
    if (metadata.isActive || evenDeletes) Some(createItem(items = toRowIdField(id).toList ::: toFields(buf))) else None
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

  def decodeSeq(buf: ByteBuffer)(implicit fmd: FieldMetaData): Seq[Option[Any]] = {
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

  def decodeValue(fmd: FieldMetaData, buf: ByteBuffer): Option[Any] = {
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

  def encodeSeq(items: Seq[Any])(implicit fmd: FieldMetaData): ByteBuffer = {
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

  def encodeValue(fmd: FieldMetaData, value_? : Option[Any]): Option[ByteBuffer] = {
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
