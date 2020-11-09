package com.qwery.database

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.ColumnTypes.IntType
import com.qwery.database.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.TableFile._
import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.models.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.models._
import com.qwery.database.types.QxInt
import com.qwery.models.expressions.{AllFields, BasicField, Expression}
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.language.postfixOps

/**
 * Represents a database table file
 * @param databaseName the name of the database
 * @param tableName    the name of the table
 * @param config       the [[TableConfig table configuration]]
 * @param device       the [[BlockDevice block device]]
 */
case class TableFile(databaseName: String, tableName: String, config: TableConfig, device: BlockDevice) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val indices = TrieMap[String, TableIndexRef]()

  // load the indices for this table
  config.indices foreach registerIndex

  /**
   * Performs a binary search of the column for a the specified value
   * @param tableIndex  the index [[TableIndexRef table index reference]]
   * @param searchValue the search value
   * @return an option of a [[Row row]]
   */
  def binarySearch(tableIndex: TableIndexRef, searchValue: Option[Any]): Option[Row] = {
    val indexColumns = List(rowIDColumn, device.columns(device.columns.indexWhere(_.name == tableIndex.indexColumn)))
    new RowOrientedFileBlockDevice(indexColumns, getTableIndexFile(databaseName, tableName, tableIndex.indexName)) use { indexDevice =>
      // create a closure to lookup a field value by row ID
      val valueAt: ROWID => Option[Any] = {
        val columnIndex = indexDevice.columns.indexWhere(_.name == tableIndex.indexColumn)
        (rowID: ROWID) => indexDevice.getField(rowID, columnIndex).value
      }

      // search for a matching field value
      var (p0: ROWID, p1: ROWID, changed: Boolean) = (0, indexDevice.length - 1, true)
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

      rowID_?.map(indexDevice.getRow)
    }
  }

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

  def count(): ROWID = device.countRows(_.isActive)

  def countRows(condition: RowTuple, limit: Option[Int] = None): Int = _iterate(condition, limit) { (_, _) => }

  /**
   * Creates a new binary search index
   * @param indexName       the name of the index
   * @param indexColumnName the name of the index [[Column column]]
   * @return a new binary search index [[BlockDevice device]]
   */
  def createIndex(indexName: String, indexColumnName: String): BlockDevice = {
    // define the columns
    val indexColumn = device.columns.find(_.name == indexColumnName).getOrElse(die(s"Column '$indexColumnName' not found"))
    val indexAllColumns = List(rowIDColumn, indexColumn)
    val sourceIndex = device.columns.indexOf(indexColumn)

    // create the index device
    val out = new RowOrientedFileBlockDevice(indexAllColumns, getTableIndexFile(databaseName, tableName, indexName))
    out.shrinkTo(0)

    // iterate the source file/table
    val eof: ROWID = device.length
    var rowID: ROWID = 0
    while (rowID < eof) {
      // build the data payload
      val indexField = device.getField(rowID, sourceIndex)
      val payloads = Seq(rowIDColumn -> QxInt(Some(rowID)), indexColumn -> indexField.typedValue) map { case (col, value) => value.encode(col) }

      // convert the payloads to binary
      val buf = allocate(out.recordSize).putRowMetadata(RowMetadata())
      payloads.zipWithIndex foreach { case (bytes, idx) =>
        buf.position(out.columnOffsets(idx))
        buf.put(bytes)
      }
      buf.flip()

      // write the index data to disk
      out.writeRow(rowID, buf)
      rowID += 1
    }

    // sort the contents of the index device
    val targetIndex = indexAllColumns.indexOf(indexColumn)
    out.sortInPlace { rowID => out.getField(rowID, targetIndex).value }

    // update the table config
    val indexRef = TableIndexRef(indexName, indexColumn.name)
    registerIndex(indexRef)
    writeTableConfig(databaseName, tableName, config.copy(indices = (config.indices ++ Seq(indexRef)).distinct))
    out
  }

  def deleteField(rowID: ROWID, columnID: Int): Boolean = {
    device.updateFieldMetaData(rowID, columnID)(_.copy(isActive = false))
    true
  }

  def deleteRow(rowID: ROWID): Int = {
    device.updateRowMetaData(rowID)(_.copy(isActive = false))
    1
  }

  def deleteRange(start: ROWID, length: Int): Int = {
    var total = 0
    val limit: ROWID = Math.min(device.length, start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      total += deleteRow(rowID)
      rowID += 1
    }
    total
  }

  def deleteRows(condition: RowTuple, limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, _) => deleteRow(rowID) }
  }

  /**
   * Exports the contents of this device as Comma Separated Values (CSV)
   * @return a new CSV [[File file]]
   */
  def exportAsCSV: File = device.exportAsCSV

  /**
   * Exports the contents of this device as JSON
   * @return a new JSON [[File file]]
   */
  def exportAsJSON: File = device.exportAsJSON

  def findRows(condition: RowTuple, limit: Option[Int] = None): List[Row] = {
    // check all available indices for the table
    val tableIndices = for {
      (searchColumn, searchValue) <- condition.toList
      tableIndex <- indices.get(searchColumn).toList
    } yield (tableIndex, searchValue)

    tableIndices.headOption match {
      // if an index was found use it
      case Some((tableIndex@TableIndexRef(indexName, indexColumn), searchValue)) =>
        logger.info(s"Using index '$tableName.$indexName' for column '${indexColumn.name}'...")
        for {
          indexedRow <- binarySearch(tableIndex, Option(searchValue)).toList
          dataRowID <- indexedRow.fields.collectFirst { case Field("rowID", _, QxInt(Some(rowID))) => rowID: ROWID }
          dataRow <- get(dataRowID)
        } yield dataRow
      // otherwise perform a table scan
      case _ =>
        var rows: List[Row] = Nil
        _iterate(condition, limit) { (_, row) => rows = row :: rows }
        rows
    }
  }

  def get(rowID: ROWID): Option[Row] = {
    val row = device.getRow(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  def getField(rowID: ROWID, columnID: Int): Field = {
    assert(device.columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    device.getField(rowID, columnID)
  }

  def getRange(start: ROWID, length: Int): Seq[Row] = {
    var rows: List[Row] = Nil
    val limit = Math.min(device.length, start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      get(rowID).foreach(row => rows = row :: rows)
      rowID += 1
    }
    rows
  }

  def getTableMetrics: TableMetrics = TableMetrics(
    databaseName = databaseName, tableName = tableName, columns = device.columns.toList.map(_.toTableColumn),
    physicalSize = device.getPhysicalSize, recordSize = device.recordSize, rows = device.length
  )

  def insertRow(values: RowTuple): ROWID = {
    val rowID = device.length
    replaceRow(rowID, values)
    rowID
  }

  def insertRows(columns: Seq[String], valueList: List[List[Any]]): Int = {
    for {
      values <- valueList
      row = RowTuple(columns zip values map { case (column, value) => column -> value }: _*)
    } replaceRow(device.length, row)
    valueList.length
  }

  def load(file: File)(transform: String => RowTuple): LoadMetrics = {
    var records: Long = 0
    val clock = stopWatch
    Source.fromFile(file).use(_.getLines() foreach { line =>
      val values = transform(line)
      if (values.nonEmpty) {
        insertRow(values)
        records += 1
      }
    })
    val ingestTime = clock()
    val recordsPerSec = records / (ingestTime / 1000)
    LoadMetrics(records, ingestTime, recordsPerSec)
  }

  def replaceRow(rowID: ROWID, values: RowTuple): Unit = {
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(RowMetadata())
    device.columns zip device.columnOffsets foreach { case (col, offset) =>
      buf.position(offset)
      val value_? = values.get(col.name)
      buf.put(Codec.encode(col, value_?))
    }
    device.writeRow(rowID, buf)
  }

  def replaceRange(start: ROWID, length: Int, values: RowTuple): Int = {
    var total = 0
    val limit: ROWID = Math.min(device.length, start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      replaceRow(rowID, values)
      total += 1
      rowID += 1
    }
    total
  }

  /**
   * Resizes the table; removing or adding rows
   */
  def resize(newSize: ROWID): Unit = device.shrinkTo(newSize)

  def selectRows(fields: Seq[Expression], where: RowTuple, limit: Option[Int] = None): QueryResult = {
    val rows = findRows(where, limit)
    val columns = device.columns.map(_.toTableColumn)
    val fieldNames: Set[String] = (fields flatMap {
      case AllFields => columns.map(_.name)
      case f: BasicField => List(f.name)
      case expression =>
        logger.error(s"Unconverted expression: $expression")
        Nil
    }).toSet

    QueryResult(databaseName, tableName, columns, __ids = rows.map(_.rowID), rows = rows map { row =>
      val mapping = row.toMap.filter { case (name, _) => fieldNames.contains(name) } // TODO properly handle field projection
      columns map { column => mapping.get(column.name) }
    })
  }

  def updateField(rowID: ROWID, columnID: Int, value: Option[Any]): Unit = {
    assert(device.columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
    device.updateField(rowID, columnID, value)
  }

  def updateRow(rowID: ROWID, values: RowTuple): Boolean = {
    get(rowID) exists { row =>
      val updatedValues = row.toRowTuple ++ values
      replaceRow(rowID, updatedValues)
      true
    }
  }

  def updateRows(values: RowTuple, condition: RowTuple, limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, row) =>
      val updatedValues = row.toRowTuple ++ values
      replaceRow(rowID, updatedValues)
    }
  }

  /**
   * Truncates the table; removing all rows
   * @return the number of rows removed
   */
  def truncate(): ROWID = {
    val oldSize = device.length
    device.shrinkTo(newSize = 0)
    oldSize
  }

  @inline
  private def isSatisfied(result: RowTuple, condition: RowTuple): Boolean = {
    condition.forall { case (name, value) => result.get(name).contains(value) }
  }

  @inline
  private def registerIndex(indexRef: TableIndexRef): Unit = {
    indices(indexRef.indexColumn) = indexRef
  }

  @inline
  private def _iterate(condition: RowTuple, limit: Option[Int] = None)(f: (ROWID, Row) => Unit): Int = {
    var matches: Int = 0
    var rowID: ROWID = 0
    val eof = device.length
    while (rowID < eof && !limit.exists(matches >= _)) {
      val row_? = get(rowID)
      row_?.foreach { row =>
        val result = RowTuple((for {field <- row.fields; value <- field.value} yield field.name -> value): _*)
        if (isSatisfied(result, condition) || condition.isEmpty) {
          f(rowID, row)
          matches += 1
        }
      }
      rowID += 1
    }
    matches
  }

}

/**
 * Table File Companion
 */
object TableFile {
  private val rowIDColumn = Column(name = "rowID", comment = "unique row ID", enumValues = Nil, ColumnMetadata(`type` = IntType))

  /**
   * Retrieves a table by name
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @return the [[TableFile]]
   */
  def apply(databaseName: String, tableName: String): TableFile = {
    val (configFile, dataFile) = (getTableConfigFile(databaseName, tableName), getTableDataFile(databaseName, tableName))
    assert(configFile.exists() && dataFile.exists(), s"Table '$tableName' does not exist")

    val config = readTableConfig(databaseName, tableName)
    val device = new RowOrientedFileBlockDevice(columns = config.columns.map(_.toColumn), dataFile)
    new TableFile(databaseName, tableName, config, device)
  }

  /**
   * Creates a new database table
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param columns      the table columns
   * @return the new [[TableFile]]
   */
  def createTable(databaseName: String, tableName: String, columns: Seq[Column]): TableFile = {
    val dataFile = getTableDataFile(databaseName, tableName)
    assert(!dataFile.exists(), s"Table '$databaseName.$tableName' already exists")

    // create the root directory
    getTableRootDirectory(databaseName, tableName).mkdirs()

    // create the table configuration file
    val config = TableConfig(columns = columns.map(_.toTableColumn), indices = Nil)
    writeTableConfig(databaseName, tableName, config)

    // return the table
    new TableFile(databaseName, tableName, config, new RowOrientedFileBlockDevice(columns, dataFile))
  }

  /**
   * Deletes the table
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param ifExists     indicates whether an existence check before attempting to delete
   * @return true, if the table was deleted
   */
  def dropTable(databaseName: String, tableName: String, ifExists: Boolean = true): Boolean = {
    val directory = getTableRootDirectory(databaseName, tableName)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TABLE CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getTableConfigFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.json")
  }

  def getTableFile(databaseName: String, tableName: String): TableFile = TableFile(databaseName, tableName)

  def getTableRootDirectory(databaseName: String, tableName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), tableName)
  }

  def getTableDataFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.qdb")
  }

  def getTableColumnFile(databaseName: String, tableName: String, columnID: Int): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName-$columnID.qdb")
  }

  def getTableIndexFile(databaseName: String, tableName: String, indexName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$indexName.qdb")
  }

  def readTableConfig(databaseName: String, tableName: String): TableConfig = {
    Source.fromFile(getTableConfigFile(databaseName, tableName)).use(src => src.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(databaseName: String, tableName: String, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(databaseName, tableName)).use(_.println(config.toJSONPretty))
  }

}