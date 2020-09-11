package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec._
import com.qwery.database.ColumnTypes._
import com.qwery.database.PersistentSeq.{Field, Row}
import com.qwery.util.OptionHelper._
import org.slf4j.LoggerFactory

import scala.reflect.{ClassTag, classTag}

/**
 * Represents a Binary Table
 */
abstract class BinaryTable[T <: Product : ClassTag] extends BinaryDevice {
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
  private val physicalColumns: List[Column] = columns.filterNot(_.isLogical)
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

  def getField(rowID: ROWID, column: Symbol): Field = {
    getField(rowID, columnIndex = columns.indexWhere(_.name == column.name))
  }

  def getField(rowID: ROWID, columnIndex: Int): Field = {
    assert(columnIndex >= 0 && columnIndex < columns.length, s"Column index ($columnIndex) is out of range")
    val (column, columnOffset) = (columns(columnIndex), columnOffsets(columnIndex))
    val buf = readBytes(rowID, numberOfBytes = column.maxLength, offset = columnOffset)
    val (fmd, value_?) = Codec.decode(buf)
    Field(name = column.name, fmd, value = value_?)
  }

  def getRow(rowID: ROWID): Row = {
    val buf = readBlock(rowID)
    Row(rowID, metadata = buf.getRowMetaData, fields = toFields(buf))
  }

  def getRows(start: ROWID, numberOfRows: Int): Seq[Row] = {
    readBlocks(start, numberOfRows) map { case (rowID, buf) =>
      Row(rowID, metadata = buf.getRowMetaData, fields = toFields(buf))
    }
  }

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = STATUS_BYTE + physicalColumns.map(_.maxLength).sum + 2 * LONG_BYTES

  def toBlocks(offset: ROWID, items: Seq[T]): Seq[(ROWID, ByteBuffer)] = {
    items.zipWithIndex.map { case (item, index) => (offset + index) -> toBytes(item) }
  }

  def toBlocks(offset: ROWID, items: Traversable[T]): Stream[(ROWID, ByteBuffer)] = {
    items.toStream.zipWithIndex.map { case (item, index) => (offset + index) -> toBytes(item) }
  }

  def toBytes(item: T): ByteBuffer = {
    val payloads = for {
      (name, value_?) <- toKeyValues(item)
      column <- nameToColumnMap.get(name).toArray if !column.isLogical
    } yield encode(column, value_?)

    // convert the row to binary
    val buf = allocate(recordSize).putRowMetaData(RowMetaData())
    payloads.zipWithIndex foreach { case (bytes, index) =>
      buf.position(columnOffsets(index))
      buf.put(bytes)
    }
    buf
  }

  def toBytes(items: Seq[T]): Seq[ByteBuffer] = items.map(item => toBytes(item))

  def toBytes(items: Traversable[T]): Stream[ByteBuffer] = items.map(item => toBytes(item)).toStream

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
              |case class StockQuote(@(ColumnInfo@field)(maxSize = 8)    symbol: String,
              |                      @(ColumnInfo@field)(maxSize = 8)    exchange: String,
              |                                                          lastSale: Double,
              |                                                          tradeTime: Long,
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

  /**
   * Truncates the table; removing all rows
   */
  def truncate(): Unit = shrinkTo(newSize = 0)

}
