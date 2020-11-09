package com.qwery.database
package device

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}
import java.text.SimpleDateFormat

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.PersistentSeq.newTempFile
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
          case Field(name, _, QxAny(None)) => name -> ""
          case Field(name, _, QxDate(Some(value))) => name -> s""""${sdf.format(value)}""""
          case Field(name, _, QxString(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxUUID(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxAny(Some(value))) => name -> value.toString
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
          case Field(name, _, QxDate(Some(value))) => name -> value.getTime
          case Field(name, _, QxString(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxUUID(Some(value))) => name -> s""""$value""""
          case Field(name, _, QxAny(Some(value))) => name -> value.toString
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
      val buf = readRow(rowID)
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
    assert(columns.indices isDefinedAt columnIndex, throw ColumnOutOfRangeException(columnIndex))
    val column = columns(columnIndex)
    val buf = readField(rowID, columnIndex)
    val (fmd, value_?) = QxAny.decode(column, buf)
    Field(name = column.name, fmd, typedValue = value_?)
  }

  def getFieldWithBinary(rowID: ROWID, columnIndex: Int): (Field, ByteBuffer) = {
    assert(columns.indices isDefinedAt columnIndex, throw ColumnOutOfRangeException(columnIndex))
    val column = columns(columnIndex)
    val buf = readField(rowID, columnIndex)
    val (fmd, value_?) = QxAny.decode(column, buf)
    Field(name = column.name, fmd, typedValue = value_?) -> buf
  }

  def getPhysicalSize: Option[Long]

  def getRow(rowID: ROWID): Row = {
    val buf = readRow(rowID)
    Row(rowID, metadata = buf.getRowMetadata, fields = toFields(buf))
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

  def readRow(rowID: ROWID): ByteBuffer

  def readRowAsFields(rowID: ROWID): BinaryRow

  def readRowMetaData(rowID: ROWID): RowMetadata

  def readRows(rowID: ROWID, numberOfRows: Int = 1): Seq[(ROWID, ByteBuffer)] = {
    for {rowID <- rowID until rowID + numberOfRows} yield rowID -> readRow(rowID)
  }

  /**
   * @return the record length in bytes
   */
  val recordSize: Int = FieldMetadata.BYTES_LENGTH + physicalColumns.map(_.maxPhysicalSize).sum

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
      item_? = if (rowID >= 0) Some(rowID -> readRow(rowID)) else None
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
    val (block0, block1) = (readRow(offset0), readRow(offset1))
    writeRow(offset0, block1)
    writeRow(offset1, block0)
  }

  def toFieldBuffers(buf: ByteBuffer): Seq[ByteBuffer] = {
    physicalColumns.zipWithIndex map { case (column, index) =>
      buf.position(columnOffsets(index))
      val fieldBytes = new Array[Byte](column.maxPhysicalSize)
      buf.get(fieldBytes)
      wrap(fieldBytes)
    }
  }

  def toFields(buf: ByteBuffer): Seq[Field] = {
    physicalColumns.zipWithIndex map { case (column, index) =>
      buf.position(columnOffsets(index))
      column.name -> QxAny.decode(column, buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

  def toFields(row: BinaryRow): Seq[Field] = {
    row.fields.zipWithIndex map { case (fieldBuf, columnIndex) =>
      val column = columns(columnIndex)
      val (fmd, typedValue) = QxAny.decode(column, fieldBuf)
      Field(column.name, fmd, typedValue)
    }
  }

  def toRowBuffer(row: BinaryRow): ByteBuffer = {
    val buf = allocate(recordSize)
    buf.putRowMetadata(row.metadata)
    row.fields zip columnOffsets foreach { case (fieldBuf, offset) =>
      buf.position(offset)
      buf.put(fieldBuf)
    }
    buf
  }

  def toRowBuffer(values: RowTuple): ByteBuffer = {
    val buf = allocate(recordSize)
    buf.putRowMetadata(RowMetadata())
    columns zip columnOffsets foreach { case (col, offset) =>
      buf.position(offset)
      val value_? = values.get(col.name)
      buf.put(Codec.encode(col, value_?))
    }
    buf
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

  def updateField(rowID: ROWID, columnID: Int, value: Option[Any]): Boolean = {
    assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    val column = columns(columnID)
    val fmd = FieldMetadata(column.metadata)
    Codec.encodeValue(column, value)(fmd) exists { buf =>
      writeField(rowID, columnID, buf)
      true
    }
  }

  def updateFieldMetaData(rowID: ROWID, columnID: Int)(update: FieldMetadata => FieldMetadata): Unit = {
    writeFieldMetaData(rowID, columnID, update(readFieldMetaData(rowID, columnID)))
  }

  def updateRowMetaData(rowID: ROWID)(update: RowMetadata => RowMetadata): Unit = {
    writeRowMetaData(rowID, update(readRowMetaData(rowID)))
  }

  def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit

  def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit

  def writeRow(rowID: ROWID, buf: ByteBuffer): Unit

  def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit

  def writeRows(blocks: Seq[(ROWID, ByteBuffer)]): Unit = blocks foreach { case (offset, buf) => writeRow(offset, buf) }

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