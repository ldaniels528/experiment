package com.qwery.database

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.ColumnTypes.IntType
import com.qwery.database.OptionComparisonHelper._
import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.models.TableIndexRef
import com.qwery.database.types.QxInt

import scala.language.postfixOps

/**
 * Creates a new table index file
 * @param ref    the [[TableIndexRef table index reference]]
 * @param device the [[BlockDevice index device]]
 */
case class TableIndexFile(ref: TableIndexRef, device: BlockDevice) {
  private val indexColumnID = device.columns.indexWhere(_.name == ref.indexColumnName)

  /**
   * Performs a binary search of the column for a the specified search value
   * @param searchValue the specified search value
   * @return an option of a [[Row row]]
   */
  def binarySearch(searchValue: Option[Any]): Option[Row] = {
    // create a closure to lookup a field value by row ID
    val valueAt: ROWID => Option[Any] = { rowID => device.getField(rowID, indexColumnID).value }

    // search for a matching field value
    var (p0: ROWID, p1: ROWID, changed: Boolean) = (0, device.length - 1, true)
    while (p0 != p1 && valueAt(p0) < searchValue && valueAt(p1) > searchValue && changed) {
      val (mp, z0, z1) = ((p0 + p1) >> 1, p0, p1)
      if (searchValue >= valueAt(mp)) p0 = mp else p1 = mp
      changed = z0 != p0 || z1 != p1
    }

    // determine whether a match was found
    val rowID_? : Option[ROWID] =
      if (valueAt(p0) == searchValue) Some(p0)
      else if (valueAt(p1) == searchValue) Some(p1)
      else None

    rowID_?.map(device.getRow)
  }

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

  /**
   * Updates the index; resorting the all values
   */
  def sortIndex(): Unit = device.sortInPlace { rowID => device.getField(rowID, indexColumnID).value }

}

/**
 * TableIndexFile Companion
 */
object TableIndexFile {
  private val rowIDColumn = Column(name = ROWID_NAME, comment = "unique row ID", enumValues = Nil, ColumnMetadata(`type` = IntType))

  /**
   * Retrieves the table index by reference
   * @param ref the [[TableIndexRef table index reference]]
   * @return the [[TableIndexFile table index]]
   */
  def apply(ref: TableIndexRef): TableIndexFile = {
    val tableConfig = TableFile.readTableConfig(ref.databaseName, ref.tableName)
    val indexColumn = tableConfig.columns.find(_.name == ref.indexColumnName).map(_.toColumn)
      .getOrElse(throw ColumnNotFoundException(ref.tableName, ref.indexColumnName))
    val indexDevice = new RowOrientedFileBlockDevice(columns = Seq(rowIDColumn, indexColumn), file = getTableIndexFile(ref))
    new TableIndexFile(ref, indexDevice)
  }

  /**
   * Creates a new binary search index
   * @param ref         the [[TableIndexRef]]
   * @param indexColumn the index [[Column column]]
   * @param source      the implicit [[BlockDevice table device]]
   * @return a new [[TableIndexFile binary search index]]
   */
  def createIndex(ref: TableIndexRef, indexColumn: Column)(implicit source: BlockDevice): TableIndexFile = {
    // define the columns
    val indexColumns = Seq(rowIDColumn, indexColumn)
    val srcColumnID = source.columns.indexWhere(_.name == indexColumn.name)

    // create the index device
    val indexDevice = new RowOrientedFileBlockDevice(indexColumns, getTableIndexFile(ref))
    indexDevice.shrinkTo(newSize = 0)

    @inline def toRowIDField(rowID: ROWID): ByteBuffer = wrap((rowID: QxInt).encode(rowIDColumn))

    // iterate the source file/table
    val eof: ROWID = source.length
    var (srcRowID: ROWID, dstRowID: ROWID) = (0, 0)
    while (srcRowID < eof) {
      // read a record from the source
      val input = source.getRow(srcRowID)
      if (input.metadata.isActive) {
        // build the data payload
        val row = BinaryRow(dstRowID, fields = Seq(toRowIDField(srcRowID), source.readField(srcRowID, srcColumnID)))

        // write the index data to disk
        indexDevice.writeRow(row)
        dstRowID += 1
      }
      srcRowID += 1
    }

    // sort the contents of the index device
    val indexFile = TableIndexFile(ref, indexDevice)
    indexFile.sortIndex()
    indexFile
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TABLE INDEX CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getTableIndexFile(databaseName: String, tableName: String, indexName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$indexName.qdb")
  }

  def getTableIndexFile(ref: TableIndexRef): File = {
    new File(new File(new File(getServerRootDirectory, ref.databaseName), ref.tableName), s"${ref.indexName}.qdb")
  }

}