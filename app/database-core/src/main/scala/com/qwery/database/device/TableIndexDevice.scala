package com.qwery.database
package device

import com.qwery.database.device.TableIndexDevice.{_encode, toRowIDField}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.ColumnTypes.IntType
import com.qwery.database.models.{BinaryRow, TableColumn, KeyValues, Row, RowMetadata}
import com.qwery.database.util.Codec
import com.qwery.database.util.OptionComparisonHelper.OptionComparator
import com.qwery.models.TableIndex

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap
import scala.collection.mutable

/**
 * Creates a new table index device
 * @param ref     the [[TableIndex table index reference]]
 * @param columns the collection of [[TableColumn columns]]
 */
class TableIndexDevice(ref: TableIndex, columns: Seq[TableColumn])
  extends RowOrientedFileBlockDevice(columns, getTableIndexFile(ref)) {
  private val indexColumnID: Int = columns.indexWhere(_.name == ref.indexColumnName)
  private val indexColumn: TableColumn = columns(indexColumnID)
  private var isDirty: Boolean = false

  /**
   * Performs a binary search of the column for a the specified search value
   * @param searchValue the specified search value
   * @return a collection of matching [[Row rows]]
   */
  def binarySearch(searchValue: Option[Any]): List[Row] = {
    // sort the index if necessary
    refresh()

    // create a closure to lookup a field value by row ID
    val valueAt: ROWID => Option[Any] = {
      val cache = mutable.Map[ROWID, Option[Any]]()
      rowID => cache.getOrElseUpdate(rowID, getField(rowID, indexColumnID).value)
    }

    // search for a matching field value
    var (p0: ROWID, p1: ROWID, changed: Boolean) = (0L, length - 1, true)
    while (p0 != p1 && valueAt(p0) < searchValue && valueAt(p1) > searchValue && changed) {
      val (mp, z0, z1) = ((p0 + p1) >> 1, p0, p1)
      if (searchValue >= valueAt(mp)) p0 = mp else p1 = mp
      changed = z0 != p0 || z1 != p1
    }

    // determine whether matches were found
    for {rowID <- (p0 to p1).toList if valueAt(rowID) == searchValue} yield getRow(rowID)
  }

  /**
   * Deletes an index row
   * @param rowID       the row ID
   * @param searchValue the specified search value
   */
  def deleteRow(rowID: ROWID, searchValue: Option[Any]): Unit = {
    binarySearch(searchValue) collectFirst { case row if row.getReferencedRowID.contains(rowID) =>
      writeRow(BinaryRow(row.id, RowMetadata(isActive = false), fields = Seq(toRowIDField(row.getReferencedRowID), toIndexField(value = None))))
    } foreach { _ => isDirty = true }
  }

  override def exportAsCSV(file: File): Unit = {
    refresh()
    super.exportAsCSV(file)
  }

  override def exportAsJSON(file: File): Unit = {
    refresh()
    super.exportAsJSON(file)
  }

  override def exportTo(out: PrintWriter)(f: Row => Option[String]): Unit = {
    refresh()
    super.exportTo(out)(f)
  }

  override def findRow(fromPos: ROWID = 0, forward: Boolean = true)(f: RowMetadata => Boolean): Option[ROWID] = {
    refresh()
    super.findRow(fromPos, forward)(f)
  }

  override def firstIndexOption: Option[ROWID] = throw UnsupportedFeature("firstIndexOption")

  override def foreach[U](callback: Row => U): Unit = {
    refresh()
    super.foreach(callback)
  }

  override def foreachBinary[U](callback: BinaryRow => U): Unit = {
    refresh()
    super.foreachBinary(callback)
  }

  override def foreachKVP[U](callback: KeyValues => U): Unit = {
    refresh()
    super.foreachKVP(callback)
  }

  override def foreachKVPInReverse[U](callback: KeyValues => U): Unit = {
    refresh()
    super.foreachKVPInReverse(callback)
  }

  /**
   * Inserts a new index row
   * @param rowID    the row ID
   * @param newValue the value to insert
   */
  def insertRow(rowID: ROWID, newValue: Option[Any]): Unit = {
    refresh()
    isDirty = true
    writeRow(models.BinaryRow(rowID, fields = Seq(toRowIDField(rowID), toIndexField(newValue))))
  }

  override def lastIndexOption: Option[ROWID] = throw UnsupportedFeature("lastIndexOption")

  /**
   * Creates a new binary search index
   * @param source the implicit [[BlockDevice table device]]
   * @return the [[TableIndexDevice]]
   */
  def rebuild()(implicit source: BlockDevice): this.type = {
    // clear the index device
    shrinkTo(newSize = 0)

    // iterate the source file/table
    val srcColumnID = source.columns.indexWhere(_.name == ref.indexColumnName)
    val eof: ROWID = source.length
    var (srcRowID: ROWID, dstRowID: ROWID) = (0L, 0L)
    while (srcRowID < eof) {
      // read a record from the source
      val input = source.getRow(srcRowID)
      if (input.metadata.isActive) {
        // build the data payload
        val row = models.BinaryRow(dstRowID, fields = Seq(toRowIDField(srcRowID), source.readField(srcRowID, srcColumnID)))

        // write the index data to disk
        writeRow(row)
        dstRowID += 1
      }
      srcRowID += 1
    }

    // finally, sort the contents of the index
    sortIndex()
    this
  }

  /**
   * Re-sorts the index if it has been modified since the last invocation
   */
  def refresh(): Unit = {
    if (isDirty) {
      sortIndex()
      isDirty = false
    }
  }

  override def reverseIterator: Iterator[(ROWID, ByteBuffer)] = throw UnsupportedFeature("reverseIterator")

  /**
   * Replaces the old value with the new value
   * @param rowID    the row ID
   * @param oldValue the old value
   * @param newValue the new value
   */
  def updateRow(rowID: ROWID, oldValue: Option[Any], newValue: Option[Any]): Unit = {
    binarySearch(oldValue) collectFirst { case row if row.getReferencedRowID.contains(rowID) =>
      writeRow(models.BinaryRow(row.id, fields = Seq(toRowIDField(row.getReferencedRowID), toIndexField(newValue))))
    } foreach (_ => isDirty = true)
  }

  /**
   * Sorts the index
   */
  @inline
  private def sortIndex(): Unit = sortInPlace(getField(_, indexColumnID).value)

  /**
   * Encodes the optional index value to binary
   * @param value the index value
   * @return the [[ByteBuffer binary representation]] of the index value
   */
  @inline
  private def toIndexField(value: Option[Any]): ByteBuffer = _encode(indexColumn, value)

}

/**
 * Table Index Device
 * @author lawrence.daniels@gmail.com
 */
object TableIndexDevice {
  private val rowIDColumn = TableColumn.create(name = ROWID_NAME, comment = Some("unique row ID"), `type` = IntType)

  /**
   * Retrieves the table index by reference
   * @param tableIndexRef the [[TableIndex table index reference]]
   * @return the [[TableIndexDevice table index]]
   */
  def apply(tableIndexRef: TableIndex): TableIndexDevice = {
    val tableConfig = readTableConfig(tableIndexRef.ref)
    val indexColumn = tableConfig.columns.find(_.name == tableIndexRef.indexColumnName)
      .getOrElse(throw ColumnNotFoundException(tableIndexRef.ref, tableIndexRef.indexColumnName))
    new TableIndexDevice(tableIndexRef, columns = Seq(rowIDColumn, indexColumn))
  }

  /**
   * Creates a new binary search index
   * @param ref         the [[TableIndex]]
   * @param indexColumn the index [[TableColumn column]]
   * @param source      the implicit [[BlockDevice table device]]
   * @return a new [[TableIndexDevice binary search index]]
   */
  def createIndex(ref: TableIndex, indexColumn: TableColumn)(implicit source: BlockDevice): TableIndexDevice = {
    new TableIndexDevice(ref, columns = Seq(rowIDColumn, indexColumn)).rebuild()
  }

  /**
   * Encodes the given indexed row ID to binary
   * @param rowID the indexed row ID
   * @return the [[ByteBuffer binary representation]] of the indexed row ID
   */
  @inline
  private def toRowIDField(rowID: ROWID): ByteBuffer = _encode(rowIDColumn, value = Some(rowID))

  /**
   * Encodes the optional indexed row ID to binary
   * @param rowID the indexed row ID
   * @return the [[ByteBuffer binary representation]] of the indexed row ID
   */
  @inline
  private def toRowIDField(rowID: Option[ROWID]): ByteBuffer = _encode(rowIDColumn, value = rowID)

  /**
   * Encodes the given value to binary
   * @param value the value
   * @return the [[ByteBuffer binary representation]] of the value
   */
  @inline
  private def _encode(column: TableColumn, value: Option[Any]): ByteBuffer = wrap(Codec.encode(column, value))

}