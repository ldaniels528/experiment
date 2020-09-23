package com.qwery.database

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

import com.qwery.database.BinarySearch.OptionComparator
import com.qwery.database.Codec.decode
import com.qwery.database.ColumnTypes._

import scala.collection.mutable
import scala.language.existentials

/**
 * Represents a raw block device
 */
trait BlockDevice {

  /**
   * Closes the underlying file handle
   */
  def close(): Unit

  def columns: List[Column]

  val columnOffsets: List[ROWID] = {
    case class Accumulator(agg: Int = 0, var last: Int = STATUS_BYTE, var list: List[Int] = Nil)
    columns.filterNot(_.isLogical).map(_.maxLength).foldLeft(Accumulator()) { (acc, maxLength) =>
      val index = acc.agg + acc.last
      acc.last = maxLength + index
      acc.list = index :: acc.list
      acc
    }.list.reverse
  }

  val nameToColumnMap: Map[String, Column] = Map(columns.map(c => c.name -> c): _*)

  val physicalColumns: List[Column] = columns.filterNot(_.isLogical)

  /**
   * defines a closure to dynamically create the optional rowID field for type T
   */
  val toRowIdField: ROWID => Option[Field] = {
    val rowIdColumn_? = columns.find(_.metadata.isRowID)
    val fmd = FieldMetadata(isCompressed = false, isEncrypted = false, isNotNull = false, `type` = ColumnTypes.LongType)
    (rowID: ROWID) => rowIdColumn_?.map(c => Field(name = c.name, fmd, value = Some(rowID)))
  }

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
   * Creates a new binary search index
   * @param indexFile the index [[File file]]
   * @param indexColumn the index [[Column column]]
   * @return a new binary search index [[BlockDevice device]]
   */
  def createIndex(indexFile: File, indexColumn: Column): BlockDevice with BinarySearch = {
    // define the columns
    val rowIDColumn = Column(name = "rowID", metadata = ColumnMetadata(`type` = IntType), maxSize = None)
    val indexAllColumns = List(rowIDColumn, indexColumn)
    val sourceIndex = columns.indexOf(indexColumn)

    // create the index device
    val out = new FileBlockDevice(indexAllColumns, indexFile) with BinarySearch

    // iterate the source file/table
    val eof: ROWID = length
    var rowID: ROWID = 0
    while (rowID < eof) {
      // build the data payload
      val indexField = getField(rowID, sourceIndex)
      val payloads = Seq(rowIDColumn -> Some(rowID), indexColumn -> indexField.value) map(t => Codec.encode(t._1, t._2))

      // convert the payloads to binary
      val buf = allocate(out.recordSize).putRowMetadata(RowMetadata())
      payloads.zipWithIndex foreach { case (bytes, idx) =>
        buf.position(out.columnOffsets(idx))
        buf.put(bytes)
      }

      // write the index data to disk
      out.writeBlock(rowID, buf)
      rowID += 1
    }

    // sort the contents of the device
    val targetIndex = indexAllColumns.indexOf(indexColumn)
    out.sortInPlace { rowID => out.getField(rowID, targetIndex).value }
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

  def foreach[U](callback: (ROWID, ByteBuffer) => U): Unit = {
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
    val buf = readBytes(rowID, numberOfBytes = column.maxLength, offset = columnOffset)
    val (fmd, value_?) = Codec.decode(buf)
    Field(name = column.name, fmd, value = value_?)
  }

  def getFieldWithBinary(rowID: ROWID, columnIndex: Int): (Field, ByteBuffer) = {
    assert(columnIndex >= 0 && columnIndex < columns.length, s"Column index ($columnIndex/${columns.size}) is out of range")
    val (column, columnOffset) = (columns(columnIndex), columnOffsets(columnIndex))
    val buf = readBytes(rowID, numberOfBytes = column.maxLength, offset = columnOffset)
    val (fmd, value_?) = Codec.decode(buf)
    Field(name = column.name, fmd, value = value_?) -> buf
  }

  def getRow(rowID: ROWID): Row = {
    val buf = readBlock(rowID)
    Row(rowID, metadata = buf.getRowMetadata, fields = toFields(buf))
  }

  def getRows(start: ROWID, numberOfRows: Int): Seq[Row] = {
    readBlocks(start, numberOfRows) map { case (rowID, buf) => Row(rowID, buf.getRowMetadata, fields = toFields(buf)) }
  }

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
    for {rowID <- rowID to rowID + numberOfBlocks} yield rowID -> readBlock(rowID)
  }

  def readByte(rowID: ROWID): Byte

  def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer

  def readRowMetaData(rowID: ROWID): RowMetadata = RowMetadata.decode(readByte(rowID))

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = STATUS_BYTE + physicalColumns.map(_.maxLength).sum

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
    private var offset: ROWID = BlockDevice.this.length - 1

    override def hasNext: Boolean = {
      offset = findRow(fromPos = offset, forward = false)(_.isActive).getOrElse(-1)
      item_? = if (offset > -1) Some(offset -> readBlock(offset)) else None
      offset -= 1
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

  def toFields(buf: ByteBuffer): List[Field] = {
    physicalColumns.zipWithIndex map { case (col, index) =>
      buf.position(columnOffsets(index))
      col.name -> decode(buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
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

  /**
   * Truncates the table; removing all rows
   */
  def truncate(): Unit = shrinkTo(newSize = 0)

  def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit

  def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): Unit = {
    blocks foreach { case (offset, buf) => writeBlock(offset, buf) }
  }

  def writeByte(rowID: ROWID, byte: Int): Unit

  def writeRowMetaData(rowID: ROWID, metaData: RowMetadata): Unit = writeByte(rowID, metaData.encode)

}
