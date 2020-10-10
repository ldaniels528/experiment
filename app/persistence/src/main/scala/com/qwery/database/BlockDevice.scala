package com.qwery.database

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.qwery.database.BlockDevice.RowStatistics
import com.qwery.database.Codec._
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.PersistentSeq.newTempFile
import com.qwery.util.ResourceHelper._

import scala.collection.mutable

/**
 * Represents a raw block device
 */
trait BlockDevice {
  val columnOffsets: List[ROWID] = {
    case class Accumulator(agg: Int = 0, var last: Int = STATUS_BYTE, var list: List[Int] = Nil)
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
    (rowID: ROWID) => rowIdColumn_?.map(c => Field(name = c.name, FieldMetadata(), value = Some(rowID)))
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
   * @return a new CSV [[File file]]
   */
  def exportAsCSV: File = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    val file = newTempFile()
    new PrintWriter(file) use { out =>
      // write the header
      val columnNames = columns.map(_.name)
      out.println(columnNames.map(s => s""""$s"""").mkString(","))

      // write all rows
      foreach { row =>
        // get the key-value pairs
        val mappings = Map(row.fields.map {
          case Field(name, _, None) => name -> ""
          case Field(name, _, Some(value: Date)) => name -> s""""${sdf.format(value)}""""
          case Field(name, _, Some(value: String)) => name -> s""""$value""""
          case Field(name, _, Some(value: UUID)) => name -> s""""$value""""
          case Field(name, _, Some(value)) => name -> value.toString
        }: _*)

        // build the line and write it
        val line = (for {name <- columnNames; value <- mappings.get(name)} yield value).mkString(",")
        out.println(line)
      }
    }
    file
  }

  /**
   * Exports the contents of this device as JSON
   * @return a new JSON [[File file]]
   */
  def exportAsJSON: File = {
    val file = newTempFile()
    new PrintWriter(file) use { out =>
      // write all rows
      foreach { row =>
        // get the key-value pairs
        val mappings = Map(row.fields collect {
          case Field(name, metadata, Some(value: Date)) => name -> value.getTime
          case Field(name, _, Some(value: String)) => name -> s""""$value""""
          case Field(name, _, Some(value: UUID)) => name -> s""""$value""""
          case Field(name, _, Some(value)) => name -> value.toString
        }: _*)

        // build the line and write it
        val line = s"{ ${mappings map { case (k, v) => s""""$k":$v""" } mkString ","} }"
        out.println(line)
      }
    }
    file
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

  def foreachBuffer[U](callback: (ROWID, ByteBuffer) => U): Unit = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof) {
      val buf = readBlock(rowID)
      if (buf.getRowMetadata.isActive) {
        buf.position(0)
        callback(rowID, buf)
      }
      rowID += 1
    }
  }

  def getField(rowID: ROWID, column: Symbol): Field = {
    getField(rowID, columnIndex = columns.indexWhere(_.name == column.name))
  }

  def getField(rowID: ROWID, columnIndex: Int): Field = {
    assert(columnIndex >= 0 && columnIndex < columns.length, s"Column index ($columnIndex/${columns.size}) is out of range")
    val (column, columnOffset) = (columns(columnIndex), columnOffsets(columnIndex))
    val buf = readBytes(rowID, numberOfBytes = column.maxPhysicalSize, offset = columnOffset)
    val (fmd, value_?) = Codec.decode(column, buf)
    Field(name = column.name, fmd, value = value_?)
  }

  def getFieldWithBinary(rowID: ROWID, columnIndex: Int): (Field, ByteBuffer) = {
    assert(columnIndex >= 0 && columnIndex < columns.length, s"Column index ($columnIndex/${columns.size}) is out of range")
    val (column, columnOffset) = (columns(columnIndex), columnOffsets(columnIndex))
    val buf = readBytes(rowID, numberOfBytes = column.maxPhysicalSize, offset = columnOffset)
    val (fmd, value_?) = Codec.decode(column, buf)
    Field(name = column.name, fmd, value = value_?) -> buf
  }

  def getPhysicalSize: Option[Long]

  def getRow(rowID: ROWID): Row = {
    val buf = readBlock(rowID)
    Row(rowID, metadata = buf.getRowMetadata, fields = toFields(buf))
  }

  def getRows(start: ROWID, numberOfRows: Int): Seq[Row] = {
    readBlocks(start, numberOfRows) map { case (rowID, buf) => Row(rowID, buf.getRowMetadata, fields = toFields(buf)) }
  }

  def getRowStatistics: RowStatistics = {
    var (active: ROWID, compressed: ROWID, deleted: ROWID, encrypted: ROWID) = (0, 0, 0, 0)
    var (rowID: ROWID, eof: ROWID) = (0, length)
    while (rowID < eof) {
      val rmd = readRowMetaData(rowID)
      if (rmd.isActive) active += 1
      if (rmd.isCompressed) compressed += 1
      if (rmd.isDeleted) deleted += 1
      if (rmd.isEncrypted) encrypted += 1
      rowID += 1
    }
    RowStatistics(active, compressed, deleted, encrypted)
  }

  /**
   * @return the length of the header
   */
  lazy val headerSize: Int = 0

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

  def readBlock(rowID: ROWID): ByteBuffer

  def readBlocks(rowID: ROWID, numberOfBlocks: Int = 1): Seq[(ROWID, ByteBuffer)] = {
    for {rowID <- rowID until rowID + numberOfBlocks} yield rowID -> readBlock(rowID)
  }

  def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer

  def readRowMetaData(rowID: ROWID): RowMetadata

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = STATUS_BYTE + physicalColumns.map(_.maxPhysicalSize).sum

  /**
   * Remove an item from the collection via its record offset
   * @param rowID the record offset
   */
  def remove(rowID: ROWID): Unit = writeRowMetaData(rowID, RowMetadata(isActive = false))

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
      item_? = if (rowID > -1) Some(rowID -> readBlock(rowID)) else None
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
  def sortInPlace[B](get: ROWID => Option[B]): this.type = {
    val cache = mutable.Map[ROWID, Option[B]]()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, get(rowID))

    def partition(low: ROWID, high: ROWID): ROWID = {
      var m = low - 1 // index of lesser item
      for (n <- low until high) if (fetch(n) < fetch(high)) {
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
    val (block0, block1) = (readBlock(offset0), readBlock(offset1))
    writeBlock(offset0, block1)
    writeBlock(offset1, block0)
  }

  def toFields(buf: ByteBuffer): Seq[Field] = {
    physicalColumns.zipWithIndex map { case (column, index) =>
      buf.position(columnOffsets(index))
      column.name -> decode(column, buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

  def fromOffset(offset: RECORD_ID): ROWID = {
    val _offset = Math.max(0, offset - headerSize)
    ((_offset / recordSize) + Math.min(1, _offset % recordSize)).toRowID
  }

  def toOffset(rowID: ROWID): RECORD_ID = rowID * recordSize + headerSize

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

  def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit

  def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): Unit = blocks foreach { case (offset, buf) => writeBlock(offset, buf) }

  def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit

}

/**
 * Block Device Companion
 */
object BlockDevice {
  val HEADER_CODE = 0xBABEFACE

  case class RowStatistics(active: ROWID, compressed: ROWID, deleted: ROWID, encrypted: ROWID) {
    def +(that: RowStatistics): RowStatistics = that.copy(
      active = that.active + active,
      compressed = that.compressed + compressed,
      deleted = that.deleted + deleted,
      encrypted = that.encrypted + encrypted
    )
  }

}