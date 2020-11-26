package com.qwery.database
package device

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.ColumnTypes.IntType
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.device.TableIndexDevice.{rowIDColumn, toRowIDField}
import com.qwery.database.models.TableIndexRef
import com.qwery.database.types.QxInt

import scala.collection.mutable

/**
 * Creates a new table index device
 * @param ref     the [[TableIndexRef table index reference]]
 * @param columns the collection of [[Column columns]]
 */
class TableIndexDevice(ref: TableIndexRef, columns: Seq[Column])
  extends RowOrientedFileBlockDevice(columns, TableIndexDevice.getTableIndexFile(ref)) {
  private val indexColumnID = columns.indexWhere(_.name == ref.indexColumnName)
  private val indexColumn = columns(indexColumnID)
  private var isDirty: Boolean = false

  /**
   * Performs a binary search of the column for a the specified search value
   * @param searchValue the specified search value
   * @return a collection of matching [[Row rows]]
   */
  def binarySearch(searchValue: Option[Any]): Seq[Row] = {
    // sort the index if necessary
    refresh()

    // create a closure to lookup a field value by row ID
    val valueAt: ROWID => Option[Any] = {
      val cache = mutable.Map[ROWID, Option[Any]]()
      rowID => cache.getOrElseUpdate(rowID, getField(rowID, indexColumnID).value)
    }

    // search for a matching field value
    var (p0: ROWID, p1: ROWID, changed: Boolean) = (0, length - 1, true)
    while (p0 != p1 && valueAt(p0) < searchValue && valueAt(p1) > searchValue && changed) {
      val (mp, z0, z1) = ((p0 + p1) >> 1, p0, p1)
      if (searchValue >= valueAt(mp)) p0 = mp else p1 = mp
      changed = z0 != p0 || z1 != p1
    }

    // determine whether matches were found
    for {rowID <- p0 to p1 if valueAt(rowID) == searchValue} yield getRow(rowID)
  }

  /**
   * Deletes an index row
   * @param rowID       the row ID
   * @param searchValue the specified search value
   */
  def deleteRow(rowID: ROWID, searchValue: Option[Any]): Unit = {
    binarySearch(searchValue) collectFirst { case row if row.getReferencedRowID.contains(rowID) =>
      writeRow(BinaryRow(row.id, RowMetadata(isActive = false), fields = Seq(wrap(Codec.encode(rowIDColumn, row.getReferencedRowID)), wrap(Codec.encode(indexColumn, value = None)))))
    } foreach { _ => isDirty = true }
  }

  override def exportAsCSV: File = {
    refresh()
    super.exportAsCSV
  }

  override def exportAsJSON: File = {
    refresh()
    super.exportAsJSON
  }

  override def findRow(fromPos: ROWID = 0, forward: Boolean = true)(f: RowMetadata => Boolean): Option[ROWID] = {
    refresh()
    super.findRow(fromPos, forward)(f)
  }

  override def firstIndexOption: Option[ROWID] = {
    refresh()
    super.firstIndexOption
  }

  override def foreach[U](callback: Row => U): Unit = {
    refresh()
    super.foreach(callback)
  }

  override def foreachBinary[U](callback: BinaryRow => U): Unit = {
    refresh()
    super.foreachBinary(callback)
  }

  /**
   * Inserts a new index row
   * @param rowID    the row ID
   * @param newValue the value to insert
   */
  def insertRow(rowID: ROWID, newValue: Option[Any]): Unit = {
    refresh()
    isDirty = true
    writeRow(BinaryRow(rowID, fields = Seq(toRowIDField(rowID), wrap(Codec.encode(indexColumn, newValue)))))
  }

  override def lastIndexOption: Option[ROWID] = {
    refresh()
    super.lastIndexOption
  }

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
    var (srcRowID: ROWID, dstRowID: ROWID) = (0, 0)
    while (srcRowID < eof) {
      // read a record from the source
      val input = source.getRow(srcRowID)
      if (input.metadata.isActive) {
        // build the data payload
        val row = BinaryRow(dstRowID, fields = Seq(toRowIDField(srcRowID), source.readField(srcRowID, srcColumnID)))

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

  /**
   * Replaces the old value with the new value
   * @param rowID    the row ID
   * @param oldValue the old value
   * @param newValue the new value
   */
  def updateRow(rowID: ROWID, oldValue: Option[Any], newValue: Option[Any]): Unit = {
    binarySearch(oldValue) collectFirst { case row if row.getReferencedRowID.contains(rowID) =>
      writeRow(BinaryRow(row.id, fields = Seq(wrap(Codec.encode(rowIDColumn, row.getReferencedRowID)), wrap(Codec.encode(indexColumn, newValue)))))
    } foreach { _ => isDirty = true }
  }

  /**
   * Sorts the index
   */
  private def sortIndex(): Unit = sortInPlace { rowID => getField(rowID, indexColumnID).value }

}

/**
 * Table Index Device
 * @author lawrence.daniels@gmail.com
 */
object TableIndexDevice {
  private val rowIDColumn = Column(name = ROWID_NAME, comment = "unique row ID", enumValues = Nil, ColumnMetadata(`type` = IntType))

  /**
   * Retrieves the table index by reference
   * @param ref the [[TableIndexRef table index reference]]
   * @return the [[TableIndexDevice table index]]
   */
  def apply(ref: TableIndexRef): TableIndexDevice = {
    val tableConfig = TableFile.readTableConfig(ref.databaseName, ref.tableName)
    val indexColumn = tableConfig.columns.find(_.name == ref.indexColumnName).map(_.toColumn)
      .getOrElse(throw ColumnNotFoundException(ref.tableName, ref.indexColumnName))
    new TableIndexDevice(ref, columns = Seq(rowIDColumn, indexColumn))
  }


  /**
   * Creates a new binary search index
   * @param ref         the [[TableIndexRef]]
   * @param indexColumn the index [[Column column]]
   * @param source      the implicit [[BlockDevice table device]]
   * @return a new [[TableIndexDevice binary search index]]
   */
  def createIndex(ref: TableIndexRef, indexColumn: Column)(implicit source: BlockDevice): TableIndexDevice = {
    // create the index device
    val indexDevice = new TableIndexDevice(ref, columns = Seq(rowIDColumn, indexColumn))
    indexDevice.rebuild()
  }

  @inline def toRowIDField(rowID: ROWID): ByteBuffer = wrap((rowID: QxInt).encode(rowIDColumn))

  //////////////////////////////////////////////////////////////////////////////////////
  //  TABLE INDEX CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getTableIndexFile(databaseName: String, tableName: String, indexColumnName: String): File = {
    val indexName = s"${tableName}_$indexColumnName"
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$indexName.qdb")
  }

  def getTableIndexFile(ref: TableIndexRef): File = {
    getTableIndexFile(ref.databaseName, ref.tableName, ref.indexColumnName)
  }

}