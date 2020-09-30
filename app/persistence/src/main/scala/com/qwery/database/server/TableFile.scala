package com.qwery.database.server

import java.io.{File, PrintWriter}
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.ColumnTypes.IntType
import com.qwery.database.OptionComparisonHelper.OptionComparator
import com.qwery.database.server.JSONSupport.{JSONProduct, JSONString}
import com.qwery.database.server.TableFile.{TableConfig, TableIndexRef, getTableIndexFile, rowIDColumn, writeTableConfig}
import com.qwery.database.{BlockDevice, Codec, Column, ColumnMetadata, ColumnTypes, FileBlockDevice, ROWID, RowMetadata}
import com.qwery.util.ResourceHelper._
import org.slf4j.LoggerFactory

import scala.collection.Map
import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.language.postfixOps

/**
 * Represents a database table
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
   * @param indexDevice the index [[BlockDevice device]]
   * @param columnName  the column name
   * @param value       the search value
   * @return an option of a row ID
   */
  def binarySearch(indexDevice: BlockDevice, columnName: String, value: Option[Any]): Option[ROWID] = {
    // create a closure to lookup a field value by row ID
    val valueAt: ROWID => Option[Any] = {
      val columnIndex = indexDevice.columns.indexWhere(_.name == columnName)
      (rowID: ROWID) => indexDevice.getField(rowID, columnIndex).value
    }

    // search for a matching field value
    var (p0: ROWID, p1: ROWID, changed: Boolean) = (0, indexDevice.length - 1, true)
    while (p0 != p1 && valueAt(p0) < value && valueAt(p1) > value && changed) {
      val (mp, z0, z1) = ((p0 + p1) / 2, p0, p1)
      if (value >= valueAt(mp)) p0 = mp else p1 = mp
      changed = z0 != p0 || z1 != p1
    }

    // determine whether a match was found
    if (valueAt(p0) == value) Some(p0)
    else if (valueAt(p1) == value) Some(p1)
    else None
  }

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

  def count(): ROWID = device.countRows(_.isActive)

  def count(condition: TupleSet, limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (_, _) => }
  }

  /**
   * Creates a new binary search index
   * @param indexName   the name of the index
   * @param indexColumn the index [[Column column]]
   * @return a new binary search index [[BlockDevice device]]
   */
  def createIndex(indexName: String, indexColumn: Column): BlockDevice = {
    // define the columns
    val indexAllColumns = List(rowIDColumn, indexColumn)
    val sourceIndex = device.columns.indexOf(indexColumn)

    // create the index device
    val out = new FileBlockDevice(indexAllColumns, getTableIndexFile(databaseName, tableName, indexName))
    out.shrinkTo(0)

    // iterate the source file/table
    val eof: ROWID = device.length
    var rowID: ROWID = 0
    while (rowID < eof) {
      // build the data payload
      val indexField = device.getField(rowID, sourceIndex)
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

    // sort the contents of the index device
    val targetIndex = indexAllColumns.indexOf(indexColumn)
    out.sortInPlace { rowID => out.getField(rowID, targetIndex).value }

    // update the table config
    val indexRef = TableIndexRef(indexName, indexColumn.name)
    registerIndex(indexRef)
    writeTableConfig(databaseName, tableName, config.copy(indices = (config.indices ++ Seq(indexRef)).distinct))
    out
  }

  def delete(rowID: ROWID): Int = {
    device.writeRowMetaData(rowID, RowMetadata(isActive = false))
    1
  }

  def deleteRange(start: ROWID, length: ROWID): Int = {
    var total = 0
    val limit: ROWID = Math.min(device.length, start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      total += delete(rowID)
      rowID += 1
    }
    total
  }

  def delete(condition: TupleSet, limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, _) => delete(rowID) }
  }

  def findRows(condition: TupleSet, limit: Option[Int] = None): List[TupleSet] = {
    // check all available indices for the table
    val tableIndex_? = (for {
      (searchColumn, searchValue) <- condition.toList
      tableIndex <- indices.get(searchColumn).toList
    } yield (tableIndex, searchValue)).headOption

    // if an index was found use it, otherwise table scan
    tableIndex_? match {
      case Some((TableIndexRef(indexName, indexColumn), value)) =>
        logger.info(s"Using index '$indexName' ($tableName) for column '${indexColumn.name}'...")
        val columns = device.columns
        val indexColumns = List(rowIDColumn, columns(columns.indexWhere(_.name == indexColumn.name)))
        new FileBlockDevice(indexColumns, getTableIndexFile(databaseName, tableName, indexName)) use { indexDevice =>
          for {
            indexedRowID <- binarySearch(indexDevice, indexColumn, Option(value)).toList
            indexRow = for {field <- indexDevice.getRow(indexedRowID).fields; value <- field.value} yield field.name -> value
            rowID <- indexRow.collectFirst { case ("rowID", rowID: ROWID) => rowID }
          } yield get(rowID)
        }
      case _ => scanForRows(condition, limit)
    }
  }

  def get(rowID: ROWID): TupleSet = {
    val row = device.getRow(rowID)
    if (row.metadata.isActive)
      (Map(ROW_ID_NAME -> rowID) ++ Map((for {field <- row.fields; value <- field.value} yield field.name -> value): _*)).asInstanceOf[TupleSet]
    else Map.empty
  }

  def getRange(start: ROWID, length: ROWID): Seq[TupleSet] = {
    var rows: List[TupleSet] = Nil
    val limit = Math.min(device.length, start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      rows = get(rowID) :: rows
      rowID += 1
    }
    rows
  }

  def insert(values: TupleSet): ROWID = {
    val rowID = device.length
    replace(rowID, values)
    rowID
  }

  def load(file: File)(transform: String => TupleSet): Long = {
    var nLines: Long = 0
    Source.fromFile(file).use(_.getLines() foreach { line =>
      val result = transform(line)
      if (result.nonEmpty) {
        insert(result)
        nLines += 1
      }
    })
    nLines
  }

  def replace(rowID: ROWID, values: TupleSet): Unit = {
    val mapping = values.map { case (k, v) => (k.name, v) }
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(RowMetadata())
    device.columns zip device.columnOffsets foreach { case (col, offset) =>
      buf.position(offset)
      val value_? = mapping.get(col.name)
      buf.put(Codec.encode(col, value_?))
    }
    device.writeBlock(rowID, buf)
  }

  def scanForRows(condition: TupleSet, limit: Option[Int] = None): List[TupleSet] = {
    var results: List[TupleSet] = Nil
    _iterate(condition, limit) { (_, result) => if(result.nonEmpty) results = result :: results }
    results
  }

  def update(values: TupleSet, condition: TupleSet, limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, result) =>
      val updatedValues = result ++ values
      replace(rowID, updatedValues)
    }
  }

  /**
   * Truncates the table; removing all rows
   */
  def truncate(): Unit = device.shrinkTo(newSize = 0)

  @inline
  private def isSatisfied(result: TupleSet, condition: TupleSet): Boolean = {
    condition.forall { case (name, value) => result.get(name).contains(value) }
  }

  @inline
  private def registerIndex(indexRef: TableIndexRef): Unit = {
    indices(indexRef.indexColumn) = indexRef
  }

  @inline
  private def _iterate(condition: TupleSet, limit: Option[Int] = None)(f: (ROWID, TupleSet) => Unit): Int = {
    val condCached = condition.map { case (symbol, value) => (symbol.name, value) }
    var matches: Int = 0
    var rowID: ROWID = 0
    val eof = device.length
    while (rowID < eof && !limit.exists(matches >= _)) {
      val result = get(rowID) ++ Map(ROW_ID_NAME -> rowID)
      if (isSatisfied(result, condCached) || condCached.isEmpty) {
        f(rowID, result)
        matches += 1
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
  private val rowIDColumn = Column(name = "rowID", comment = "unique row ID", ColumnMetadata(`type` = IntType))

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
    val device = new FileBlockDevice(columns = config.columns.map(_.toColumn), dataFile)
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
    new TableFile(databaseName, tableName, config, new FileBlockDevice(columns, dataFile))
  }

  /**
   * Deletes the table
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @return true, if the table was deleted
   */
  def dropTable(databaseName: String, tableName: String): Boolean = {
    val directory = getTableRootDirectory(databaseName, tableName)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

  def getDatabaseRootDirectory(databaseName: String): File = {
    new File(getServerRootDirectory, databaseName)
  }

  def getServerRootDirectory: File = {
    val directory = new File(sys.env.getOrElse("QWERY_DB", "qwery_db"))
    assert(directory.mkdirs() || directory.exists(), s"Could not create data directory - ${directory.getAbsolutePath}")
    directory
  }

  def getTableConfigFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.json")
  }

  def getTableRootDirectory(databaseName: String, tableName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), tableName)
  }

  def getTableDataFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.qdb")
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

  final implicit class ColumnToTableColumnConversion(val column: Column) extends AnyVal {
    @inline
    def toTableColumn: TableColumn = TableColumn(
      name = column.name,
      `type` = column.metadata.`type`.toString,
      comment = if (column.comment.nonEmpty) Some(column.comment) else None,
      sizeInBytes = column.sizeInBytes,
      isCompressed = column.metadata.isCompressed,
      isEncrypted = column.metadata.isEncrypted,
      isNullable = column.metadata.isNullable,
      isPrimary = column.metadata.isPrimary,
      isRowID = column.metadata.isRowID,
    )
  }

  final implicit class TableColumnToColumnConversion(val column: TableColumn) extends AnyVal {
    @inline
    def toColumn: Column = new Column(
      name = column.name,
      comment = column.comment.getOrElse(""),
      sizeInBytes = column.sizeInBytes,
      metadata = ColumnMetadata(
        `type` = ColumnTypes.withName(column.`type`),
        isCompressed = column.isCompressed,
        isEncrypted = column.isEncrypted,
        isNullable = column.isNullable,
        isPrimary = column.isPrimary,
        isRowID = column.isRowID
      ))
  }

  case class DatabaseMetrics(databaseName: String,
                             tables: Seq[String],
                             responseTimeMillis: Double = 0)

  case class TableColumn(name: String,
                         `type`: String,
                         comment: Option[String],
                         sizeInBytes: Int,
                         isCompressed: Boolean,
                         isEncrypted: Boolean,
                         isNullable: Boolean,
                         isPrimary: Boolean,
                         isRowID: Boolean)

  case class TableConfig(columns: Seq[TableColumn], indices: Seq[TableIndexRef])

  case class TableIndexRef(indexName: String, indexColumn: String)

  case class TableMetrics(databaseName: String,
                          tableName: String,
                          columns: Seq[TableColumn],
                          physicalSize: Option[Long],
                          recordSize: Int,
                          rows: ROWID,
                          responseTimeMillis: Double = 0)

}