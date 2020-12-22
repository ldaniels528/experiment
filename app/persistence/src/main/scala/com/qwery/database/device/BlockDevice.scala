package com.qwery.database
package device

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.KeyValues.isSatisfied
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.device.BlockDevice.RowStatistics
import com.qwery.database.types._
import com.qwery.util.ResourceHelper._

import scala.collection.mutable

/**
 * Represents a raw block device
 */
trait BlockDevice {
  val columnOffsets: List[ROWID] = {
    case class Accumulator(agg: Int = 0, var last: Int = FieldMetadata.BYTES_LENGTH, var list: List[Int] = Nil)
    columns.filterNot(_.isLogical).map(_.maxPhysicalSize).foldLeft(Accumulator()) { (acc, maxLength) =>
      val index = acc.agg + acc.last
      acc.last = maxLength + index
      acc.list = index :: acc.list
      acc
    }.list.reverse
  }

  val nameToColumnMap: Map[String, Column] = Map(columns.map(c => c.name -> c): _*)

  val physicalColumns: Seq[Column] = columns.filterNot(_.isLogical)

  /**
   * defines a closure to dynamically create the optional rowID field for type T
   */
  val toRowIdField: ROWID => Option[Field] = {
    val rowIdColumn_? = columns.find(_.metadata.isRowID)
    (rowID: ROWID) => rowIdColumn_?.map(c => Field(name = c.name, FieldMetadata(), typedValue = QxInt(Some(rowID))))
  }

  /**
   * Closes the underlying file handle
   */
  def close(): Unit

  def columns: Seq[Column]

  /**
   * Counts the number of rows matching the predicate
   * @param predicate the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  def countRows(predicate: RowMetadata => Boolean): ROWID = {
    val eof: ROWID = length
    var (rowID: ROWID, total) = (0, 0)
    while (rowID < eof) {
      if (predicate(readRowMetaData(rowID))) total += 1
      rowID += 1
    }
    total
  }

  /**
   * Exports the contents of this device as Comma Separated Values (CSV)
   * @param file the destination [[File file]]
   */
  def exportAsCSV(file: File): Unit = {
    new PrintWriter(file) use { out =>
      // write the header
      val columnNames = columns.map(_.name)
      out.println(columnNames.map(s => s""""$s"""").mkString(","))

      // write all rows
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
      exportTo(out) { row =>
        // convert the row to key-value pairs
        val mappings = Map(row.fields.map {
          case Field(name, _, QxAny(None)) => name -> ""
          case Field(name, _, QxDate(Some(value))) => name -> s""""${sdf.format(value)}""""
          case Field(name, _, QxString(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxUUID(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxAny(Some(value))) => name -> value.toString
        }: _*)

        // build the line as CSV
        val line = (for {name <- columnNames; value <- mappings.get(name)} yield value).mkString(",")
        Some(line)
      }
    }
  }

  /**
   * Exports the contents of this device as JSON
   * @param file the destination [[File file]]
   */
  def exportAsJSON(file: File): Unit = {
    new PrintWriter(file) use { out =>
      exportTo(out) { row =>
        // convert the row to key-value pairs
        val mappings = Map(row.fields collect {
          case Field(name, _, QxAny(None)) => name -> "null"
          case Field(name, _, QxDate(Some(value))) => name -> value.getTime
          case Field(name, _, QxString(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxUUID(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxAny(Some(value))) => name -> value.toString
        }: _*)

        // build the line as JSON
        val line = s"{ ${mappings map { case (k, v) => s""""$k":$v""" } mkString ","} }"
        Some(line)
      }
    }
  }

  /**
   * Exports the contents of this device to a stream
   * @param out the [[PrintWriter stream]]
   * @param f   the transformation function
   */
  def exportTo(out: PrintWriter)(f: Row => Option[String]): Unit = {
    foreach { row =>
      f(row) foreach out.println
    }
  }

  def findRow(fromPos: ROWID = 0, forward: Boolean = true)(f: RowMetadata => Boolean): Option[ROWID] = {
    var rowID = fromPos
    if (forward) while (rowID < length && !f(readRowMetaData(rowID))) rowID += 1
    else while (rowID >= 0 && !f(readRowMetaData(rowID))) rowID -= 1
    if (rowID >= 0 && rowID < length) Some(rowID) else None
  }

  def firstIndexOption: Option[ROWID] = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof && readRowMetaData(rowID).isDeleted) rowID += 1
    if (rowID < eof) Some(rowID) else None
  }

  def foreach[U](callback: Row => U): Unit = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof) {
      val row = getRow(rowID)
      if (row.metadata.isActive) callback(row)
      rowID += 1
    }
  }

  def foreachBinary[U](callback: BinaryRow => U): Unit = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof) {
      val row = readRow(rowID)
      if (row.metadata.isActive) callback(row)
      rowID += 1
    }
  }

  def foreachKVP[U](callback: KeyValues => U): Unit = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof) {
      val row = getRow(rowID)
      if (row.metadata.isActive) callback(row.toKeyValues)
      rowID += 1
    }
  }

  /**
   * Retrieves a column by ID
   * @param columnID the column ID
   * @return the [[Column column]]
   */
  def getColumnByID(columnID: Int): Column = columns(columnID)

  /**
   * Retrieves a column by name
   * @param name the column name
   * @return the [[Column column]]
   */
  def getColumnByName(name: String): Column = {
    columns.find(_.name == name).getOrElse(throw ColumnNotFoundException(tableName = "???", columnName = name))
  }

  def getField(rowID: ROWID, column: Symbol): Field = {
    getField(rowID, columnID = columns.indexWhere(_.name == column.name))
  }

  def getField(rowID: ROWID, columnID: Int): Field = {
    assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    val column = columns(columnID)
    val buf = readField(rowID, columnID)
    val (fmd, value_?) = QxAny.decode(column, buf)
    Field(name = column.name, fmd, typedValue = value_?)
  }

  def getKeyValues(rowID: ROWID): Option[KeyValues] = KeyValues(buf = readRowAsBinary(rowID))(this)

  def getPhysicalSize: Option[Long]

  def getRow(rowID: ROWID): Row = {
    val buf = readRowAsBinary(rowID)
    Row(rowID, metadata = buf.getRowMetadata, fields = Row.toFields(buf)(this))
  }

  def getRowStatistics: RowStatistics = {
    var (active: ROWID, compressed: ROWID, deleted: ROWID, encrypted: ROWID, locked: ROWID, replicated: ROWID) = (0, 0, 0, 0, 0, 0)
    var (rowID: ROWID, eof: ROWID) = (0, length)
    while (rowID < eof) {
      val rmd = readRowMetaData(rowID)
      if (rmd.isActive) active += 1
      if (rmd.isCompressed) compressed += 1
      if (rmd.isDeleted) deleted += 1
      if (rmd.isEncrypted) encrypted += 1
      if (rmd.isLocked) locked += 1
      if (rmd.isReplicated) replicated += 1
      rowID += 1
    }
    RowStatistics(active, compressed, deleted, encrypted, locked, replicated)
  }

  /**
   * @return the last active row ID or <tt>None</tt>
   */
  def lastIndexOption: Option[ROWID] = {
    var rowID: ROWID = length - 1
    while (rowID >= 0 && readRowMetaData(rowID).isDeleted) rowID -= 1
    if (rowID >= 0) Some(rowID) else None
  }

  /**
   * @return the number of records in the file, including the deleted ones.
   *         The [[PersistentSeq.count count()]] method is probably the method you truly want.
   * @see [[PersistentSeq.count]]
   */
  def length: ROWID

  def readColumnMetaData(columnID: Int): ColumnMetadata = {
    assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    columns(columnID).metadata
  }

  def readField(rowID: ROWID, columnID: Int): ByteBuffer

  def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata

  def readRowAsBinary(rowID: ROWID): ByteBuffer

  def readRow(rowID: ROWID): BinaryRow

  def readRowMetaData(rowID: ROWID): RowMetadata

  def readRows(rowID: ROWID, numberOfRows: Int = 1): Seq[BinaryRow] = {
    for (rowID <- rowID until rowID + numberOfRows) yield readRow(rowID)
  }

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = RowMetadata.BYTES_LENGTH + physicalColumns.map(_.maxPhysicalSize).sum

  /**
   * Remove an item from the collection via its record offset
   * @param rowID the record offset
   */
  def remove(rowID: ROWID): Unit = updateRowMetaData(rowID)(_.copy(isActive = false))

  def reverseInPlace(): Unit = {
    var (top: ROWID, bottom: ROWID) = (0, length - 1)
    while (top < bottom) {
      swap(top, bottom)
      bottom -= 1
      top += 1
    }
  }

  def reverseIterator: Iterator[(ROWID, ByteBuffer)] = new Iterator[(ROWID, ByteBuffer)] {
    private var item_? : Option[(ROWID, ByteBuffer)] = None
    private var rowID: ROWID = BlockDevice.this.length - 1

    override def hasNext: Boolean = {
      rowID = findRow(fromPos = rowID, forward = false)(_.isActive).getOrElse(-1)
      item_? = if (rowID >= 0) Some(rowID -> readRowAsBinary(rowID)) else None
      rowID -= 1
      item_?.nonEmpty
    }

    override def next: (ROWID, ByteBuffer) = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }
  }

  def shrinkTo(newSize: ROWID): Unit

  /**
   * Performs an in-place sorting of the collection
   * @param get the row ID to value extraction function
   */
  def sortInPlace[B](get: ROWID => Option[B], isAscending: Boolean = true): this.type = {
    val cache = mutable.Map[ROWID, Option[B]]()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, get(rowID))

    def isLesser(n: Int, high: Int): Boolean = if(isAscending) fetch(n) > fetch(high) else fetch(n) < fetch(high)

    def partition(low: ROWID, high: ROWID): ROWID = {
      var m = low - 1 // index of lesser item
      for (n <- low until high) if (isLesser(n, high)) {
        m += 1 // increment the index of lesser item
        swap(m, n)
      }
      swap(m + 1, high)
      m + 1
    }

    def sort(low: ROWID, high: ROWID): Unit = if (low < high) {
      val pi = partition(low, high)
      sort(low, pi - 1)
      sort(pi + 1, high)
    }

    def swap(offset0: ROWID, offset1: ROWID): Unit = {
      val (elem0, elem1) = (cache.remove(offset0), cache.remove(offset1))
      BlockDevice.this.swap(offset0, offset1)
      elem0.foreach(v => cache(offset1) = v)
      elem1.foreach(v => cache(offset0) = v)
    }

    sort(low = 0, high = BlockDevice.this.length - 1)
    this
  }

  def swap(offset0: ROWID, offset1: ROWID): Unit = {
    val (block0, block1) = (readRowAsBinary(offset0), readRowAsBinary(offset1))
    writeRowAsBinary(offset0, block1)
    writeRowAsBinary(offset1, block0)
  }

  /**
   * Trims dead entries from of the collection
   * @return the new size of the file
   */
  def trim(): ROWID = {
    var rowID: ROWID = length - 1
    while (rowID >= 0 && readRowMetaData(rowID).isDeleted) rowID -= 1
    val newLength = rowID + 1
    shrinkTo(newLength)
    newLength
  }

  def toList: List[Row] = {
    var list: List[Row] = Nil
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof) {
      val row = getRow(rowID)
      if (row.metadata.isActive) list = row :: list
      rowID += 1
    }
    list
  }

  def updateField(rowID: ROWID, columnID: Int, value: Option[Any]): Unit = {
    assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    val column = columns(columnID)
    val fmd = FieldMetadata(column.metadata)
    Codec.encodeValue(column, value)(fmd).foreach(writeField(rowID, columnID, _))
  }

  def updateFieldMetaData(rowID: ROWID, columnID: Int)(update: FieldMetadata => FieldMetadata): Unit = {
    writeFieldMetaData(rowID, columnID, update(readFieldMetaData(rowID, columnID)))
  }

  def updateRowMetaData(rowID: ROWID)(update: RowMetadata => RowMetadata): Unit = {
    writeRowMetaData(rowID, update(readRowMetaData(rowID)))
  }

  def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit

  def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit

  def writeRow(row: BinaryRow): Unit = writeRowAsBinary(row.id, row.toRowBuffer(this))

  def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit

  def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit

  def writeRows(rows: Seq[BinaryRow]): Unit = rows foreach writeRow

  //////////////////////////////////////////////////////////////////
  //      UTILITIES
  //////////////////////////////////////////////////////////////////

  def whileKV(condition: KeyValues)(f: KeyValues => Boolean): Int = {
    var (matches: Int, rowID: ROWID) = (0, 0)
    val eof = length
    var proceed = true
    while (rowID < eof && proceed) {
      getKeyValues(rowID) foreach { row =>
        if (condition.isEmpty || isSatisfied(row, condition)) {
          proceed = f(row)
          if (proceed) matches += 1
        }
      }
      rowID += 1
    }
    matches
  }

  def whileRow(condition: KeyValues)(f: Row => Boolean): Int = {
    var (matches: Int, rowID: ROWID) = (0, 0)
    val eof = length
    var proceed = true
    while (rowID < eof && proceed) {
      val row = getRow(rowID)
      if (row.metadata.isActive) {
        if (condition.isEmpty || isSatisfied(row.toKeyValues, condition)) {
          proceed = f(row)
          if (proceed) matches += 1
        }
      }
      rowID += 1
    }
    matches
  }

}

/**
 * Block Device Companion
 */
object BlockDevice {

  case class RowStatistics(active: ROWID, compressed: ROWID, deleted: ROWID, encrypted: ROWID, locked: ROWID, replicated: ROWID) {
    def +(that: RowStatistics): RowStatistics = that.copy(
      active = that.active + active,
      compressed = that.compressed + compressed,
      deleted = that.deleted + deleted,
      encrypted = that.encrypted + encrypted,
      locked = that.locked + locked,
      replicated = that.replicated + replicated
    )
  }

}