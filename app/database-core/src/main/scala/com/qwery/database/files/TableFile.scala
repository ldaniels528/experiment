package com.qwery.database.files

import com.qwery.database.collections.PersistentSeq
import com.qwery.database.device.{BlockDevice, BlockDeviceQuery, RowOrientedFileBlockDevice, TableIndexDevice, TableIndexRef}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.{Column, Field, KeyValues, ROWID, ROWID_NAME, RecursiveFileList, Row, RowIsLockedException, createTempTable, die, stopWatch}
import com.qwery.models.OrderColumn
import com.qwery.models.expressions.{Condition, Expression, Field => SQLField}
import com.qwery.util.ResourceHelper._

import java.io.File
import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.reflect.ClassTag

/**
 * Represents a physical table file
 * @param databaseName the name of the database
 * @param tableName    the name of the table
 * @param config       the [[TableConfig table configuration]]
 * @param device       the [[BlockDevice block device]]
 */
case class TableFile(databaseName: String, tableName: String, config: TableConfig, device: BlockDevice) {
  private val indexFiles = TrieMap[String, TableIndexDevice]()
  private val selector = new BlockDeviceQuery(device)

  // load the indices for this table
  config.indices.foreach(ref => registerIndex(ref, TableIndexDevice(ref)))

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

  def count(): Long = device.countRows(_.isActive)

  def countRows(condition: KeyValues, limit: Option[Int] = None): Long = device.whileRow(condition, limit) { _ => }

  /**
   * Creates a new binary search index
   * @param indexColumnName the name of the index [[Column column]]
   * @return a new binary search [[TableIndexDevice index]]
   */
  def createIndex(indexColumnName: String): TableIndexDevice = {
    val indexRef = TableIndexRef(databaseName, tableName, indexColumnName)
    val indexColumn = getColumnByName(indexRef.indexColumnName)
    val tableIndex = TableIndexDevice.createIndex(indexRef, indexColumn)(device)
    registerIndex(indexRef, tableIndex)
    writeTableConfig(databaseName, tableName, config.copy(indices = (indexRef :: config.indices.toList).distinct))
    tableIndex
  }

  def deleteField(rowID: ROWID, columnID: Int): Unit = {
    _indexed(rowID, columnID) { _ => device.updateFieldMetaData(rowID, columnID)(_.copy(isActive = false)) } {
      (indexDevice, searchValue) => indexDevice.deleteRow(rowID, searchValue)
    }
  }

  def deleteField(rowID: ROWID, columnName: String): Unit = deleteField(rowID, getColumnIdByName(columnName))

  def deleteRange(start: ROWID, length: Int): Long = {
    val limit: ROWID = device.length min (start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      deleteRow(rowID)
      rowID += 1
    }
    limit - start
  }

  def deleteRow(rowID: ROWID): Unit = {
    _indexed(rowID) { _ => device.updateRowMetaData(rowID)(_.copy(isActive = false)) } {
      (indexDevice, searchValue) => indexDevice.deleteRow(rowID, searchValue)
    }
  }

  def deleteRows(condition: KeyValues, limit: Option[Int] = None): Long = {
    device.whileRow(condition, limit) { row => deleteRow(row.id) }
  }

  /**
   * Exports the contents of this device as Comma Separated Values (CSV)
   * @return a new CSV [[File file]]
   */
  def exportAsCSV(file: File): Unit = device.exportAsCSV(file)

  /**
   * Exports the contents of this device as JSON
   * @return a new JSON [[File file]]
   */
  def exportAsJSON(file: File): Unit = device.exportAsJSON(file)

  /**
   * Atomically retrieves and replaces a row by ID
   * @param rowID the row ID
   * @param f     the update function to execute
   * @return the [[Row]] representing the replaced record
   */
  def fetchAndReplace(rowID: ROWID)(f: KeyValues => KeyValues): Row = {
    val input = getRow(rowID).map(_.toKeyValues).getOrElse(KeyValues(ROWID_NAME -> rowID))
    val output = f(input)
    replaceRow(rowID, output)
    output.toBinaryRow(rowID)(device).toRow(device)
  }

  /**
   * Atomically retrieves and updates a row by ID
   * @param rowID the row ID
   * @param f     the update function to execute
   * @return the option of a [[Row]] representing the updated record
   */
  def fetchAndUpdate(rowID: ROWID)(f: KeyValues => KeyValues): Option[Row] = {
    getRow(rowID).map(_.toKeyValues).map(f) foreach(updateRow(rowID, _))
    getRow(rowID)
  }

  /**
   * Atomically retrieves and updates rows that satisfy the given condition
   * @param condition the inclusion condition
   * @param f     the update function to execute
   * @return the [[BlockDevice]] containing the updated rows
   */
  def fetchAndUpdate(condition: KeyValues)(f: KeyValues => KeyValues): BlockDevice = {
    val rows = getRows(condition)
    rows foreachKVP { kvp =>
      rows.writeRow(f(kvp).toBinaryRow(rows))
    }
    rows
  }

  /**
   * Retrieves a column by ID
   * @param columnID the column ID
   * @return the [[Column column]]
   */
  def getColumnByID(columnID: Int): Column = device.getColumnByID(columnID)

  /**
   * Retrieves a column by name
   * @param name the column name
   * @return the [[Column column]]
   */
  def getColumnByName(name: String): Column = device.getColumnByName(name)

  /**
   * Retrieves a column ID
   * @param name the column name
   * @return the [[Column column]]
   */
  def getColumnIdByName(name: String): Int = device.columns.indexWhere(_.name == name)

  /**
   * Retrieves a field by row and column IDs
   * @param rowID    the row ID
   * @param columnID the column ID
   * @return the [[Field field]]
   */
  def getField(rowID: ROWID, columnID: Int): Field = device.getField(rowID, columnID)

  /**
   * Retrieves key-values by row ID
   * @param rowID the row ID
   * @return the option of [[KeyValues key-values]]
   */
  def getKeyValues(rowID: ROWID): Option[KeyValues] = device.getKeyValues(rowID)

  /**
   * Retrieves a row by ID
   * @param rowID the row ID
   * @return the option of a [[Row row]]
   */
  def getRow(rowID: ROWID): Option[Row] = {
    val row = device.getRow(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  /**
   * Retrieves rows matching the given condition up to the optional limit
   * @param condition the given [[KeyValues condition]]
   * @param limit     the optional limit
   * @return the [[BlockDevice results]]
   */
  def getRows(condition: KeyValues, limit: Option[Int] = None): BlockDevice = {
    implicit val results: BlockDevice = createTempTable(device.columns)

    // check all available indices for the table
    val tableIndex_? = (for {
      (searchColumn, searchValue) <- condition.items
      indexDevice <- indexFiles.get(searchColumn)
    } yield (indexDevice, searchValue)).headOption

    tableIndex_? match {
      // if an index was found use it
      case Some((indexDevice, searchValue)) =>
        for {
          indexedRow <- indexDevice.binarySearch(Option(searchValue))
          dataRowID <- indexedRow.getReferencedRowID
          dataRow <- getRow(dataRowID)
        } results.writeRow(dataRow.toBinaryRow)

      // otherwise perform a table scan
      case _ => device.whileRow(condition, limit) { row => results.writeRow(row.toBinaryRow) }
    }
    results
  }

  /**
   * Retrieves a range of rows
   * @param start the initial row ID of the range
   * @param length the number of rows to retrieve
   * @return a [[BlockDevice]] containing the rows
   */
  def getRange(start: ROWID, length: Int): BlockDevice = {
    val rows = createTempTable(device.columns)
    val limit = device.length min (start + length)
    var rowID: ROWID = start
    while (rowID < limit) {
      val row = device.readRow(rowID)
      if (row.metadata.isActive) rows.writeRow(row)
      rowID += 1
    }
    rows
  }

  def getTableMetrics: TableMetrics = TableMetrics(
    databaseName = databaseName, tableName = tableName, columns = device.columns.toList.map(_.toTableColumn),
    physicalSize = device.getPhysicalSize, recordSize = device.recordSize, rows = device.length
  )

  /**
   * Facilitates a line-by-line ingestion of a text file
   * @param file      the text [[File file]]
   * @param transform the [[String line]]-to-[[KeyValues record]] transformation function
   * @return the [[LoadMetrics]]
   */
  def ingestTextFile(file: File)(transform: String => Option[KeyValues]): LoadMetrics = {
    var records: Long = 0
    val clock = stopWatch
    Source.fromFile(file).use(src =>
      for {
        line <- src.getLines() if line.nonEmpty
        row <- transform(line) if row.nonEmpty
      } {
        insertRow(row)
        records += 1
      })
    val ingestTime = clock()
    LoadMetrics(records, ingestTime, recordsPerSec = records / (ingestTime / 1000))
  }

  def insertRow(values: KeyValues): ROWID = {
    val rowID = device.length
    _indexed(rowID, values) { _ => device.writeRowAsBinary(rowID, values.toRowBuffer(device)) } {
      (indexDevice, _, newValue) => indexDevice.insertRow(rowID, newValue)
    }
    rowID
  }

  def insertRows(rows: BlockDevice): Int = {
    var inserted = 0
    rows foreach { row =>
      insertRow(row.toKeyValues)
      inserted += 1
    }
    inserted
  }

  def insertRows(columns: Seq[String], rowValues: Seq[Seq[Any]]): Seq[ROWID] = {
    for {
      values <- rowValues
      row = KeyValues(columns zip values map { case (column, value) => column -> value }: _*)
    } yield insertRow(row)
  }

  def lockRow(rowID: ROWID): Unit = {
    device.updateRowMetaData(rowID) { rmd =>
      if (rmd.isLocked) throw RowIsLockedException(rowID) else rmd.copy(isLocked = true)
    }
  }

  def replaceRange(start: ROWID, length: Int, values: KeyValues): Unit = {
    val limit: ROWID = start + length
    var rowID: ROWID = start
    while (rowID < limit) {
      replaceRow(rowID, values)
      rowID += 1
    }
  }

  def replaceRow(rowID: ROWID, values: KeyValues): Unit = {
    _indexed(rowID, values) { _ => device.writeRowAsBinary(rowID, values.toRowBuffer(device)) } {
      (indexDevice, oldValue, newValue) => indexDevice.updateRow(rowID, oldValue, newValue)
    }
  }

  /**
   * Resizes the table; removing or adding rows
   */
  def resize(newSize: ROWID): Unit = device.shrinkTo(newSize)

  /**
    * Executes a query
    * @param fields  the [[Expression field projection]]
    * @param where   the condition which determines which records are included
    * @param groupBy the optional aggregation columns
    * @param orderBy the columns to order by
    * @param limit   the optional limit
    * @return a [[BlockDevice]] containing the rows
   */
  def selectRows(fields: Seq[Expression],
                 where: KeyValues,
                 groupBy: Seq[SQLField] = Nil,
                 having: Option[Condition] = None,
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    selector.select(fields, where, groupBy, having, orderBy, limit)
  }

  def unlockRow(rowID: ROWID): Unit = {
    device.updateRowMetaData(rowID) { rmd =>
      if (rmd.isLocked) rmd.copy(isLocked = false) else throw RowIsLockedException(rowID)
    }
  }

  def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): Unit = {
    _indexed(rowID, columnID) { _ => device.updateField(rowID, columnID, newValue) } {
      (indexDevice, oldValue) => indexDevice.updateRow(rowID, oldValue, newValue)
    }
  }

  def updateField(rowID: ROWID, columnName: String, newValue: Option[Any]): Unit = {
    updateField(rowID, getColumnIdByName(columnName), newValue)
  }

  def updateRange(start: ROWID, length: Int, values: KeyValues): Long = {
    val limit: ROWID = (start + length) min device.length
    var rowID: ROWID = start
    while (rowID < limit) {
      updateRow(rowID, values)
      rowID += 1
    }
    limit - start
  }

  def updateRow(rowID: ROWID, values: KeyValues): Unit = {
    _indexed(rowID, values) { row_? =>
      row_? foreach { row =>
        val updatedValues = row.toKeyValues ++ values
        replaceRow(rowID, updatedValues)
      }
    } { (indexDevice, oldValue, newValue) => indexDevice.updateRow(rowID, oldValue, newValue) }
  }

  def updateRows(values: KeyValues, condition: KeyValues, limit: Option[Int] = None): Long = {
    device.whileRow(condition, limit) { row =>
      val updatedValues = row.toKeyValues ++ values
      replaceRow(row.id, updatedValues)
    }
  }

  def toPersistentSeq[A <: Product : ClassTag]: PersistentSeq[A] = {
    val (columns, _class) = BlockDevice.toColumns[A]
    val deviceColumns = device.columns.map(c => c.name -> c.metadata.`type`)
    val productColumns = columns.map(c => c.name -> c.metadata.`type`)
    val missingColumns = deviceColumns.collect { case t@(name, _type) if !deviceColumns.contains(t) => name + ':' + _type }
    assert(missingColumns.isEmpty, s"Class ${_class.getName} does not contain columns: ${missingColumns.mkString(", ")}")
    new PersistentSeq[A](device, _class)
  }

  /**
   * Truncates the table; removing all rows
   * @return the number of rows removed
   */
  def truncate(): ROWID = {
    // shrink the table to zero
    val oldSize = device.length
    device.shrinkTo(newSize = 0)
    oldSize
  }

  @inline
  private def registerIndex(indexRef: TableIndexRef, device: TableIndexDevice): Unit = {
    indexFiles(indexRef.indexColumnName) = device
  }

  @inline
  private def _indexed[A](rowID: ROWID, columnID: Int)(mutation: Option[Row] => A)(f: (TableIndexDevice, Option[Any]) => Unit): A = {
    // first get the pre-updated value
    val row_? = if (indexFiles.nonEmpty) getRow(rowID) else None

    // execute the update operation
    val result = mutation(row_?)

    // update the affected indices
    if (indexFiles.nonEmpty) {
      val oldValue = row_?.flatMap(_.fields(columnID).value)
      val columnName = getColumnByID(columnID).name
      indexFiles foreach {
        case (indexColumn, indexDevice) if columnName == indexColumn => f(indexDevice, oldValue)
        case _ =>
      }
    }
    result
  }

  @inline
  private def _indexed[A](rowID: ROWID)(mutation: Option[Row] => A)(f: (TableIndexDevice, Option[Any]) => Unit): A = {
    // first get the pre-updated value
    val row_? = if (indexFiles.nonEmpty) getRow(rowID) else None

    // execute the update operation
    val result = mutation(row_?)

    // update the affected indices
    if (indexFiles.nonEmpty) {
      val oldValues: Seq[(TableIndexDevice, Option[Any])] = {
        indexFiles.toSeq map { case (indexColumn, indexDevice) =>
          val indexColumnID = getColumnIdByName(indexColumn)
          val oldValue = row_?.flatMap(_.fields(indexColumnID).value)
          (indexDevice, oldValue)
        }
      }
      oldValues foreach {
        case (indexDevice, oldValue) => f(indexDevice, oldValue)
        case _ =>
      }
    }
    result
  }

  @inline
  private def _indexed[A](rowID: ROWID, values: KeyValues)(mutation: Option[Row] => A)(f: (TableIndexDevice, Option[Any], Option[Any]) => Unit): A = {
    // first get the pre-updated values
    val row_? = if (indexFiles.nonEmpty) getRow(rowID) else None

    // execute the update operation
    val result = mutation(row_?)

    // update the affected indices
    if (indexFiles.nonEmpty) {
      val oldAndNewValues: Seq[(TableIndexDevice, Option[Any], Option[Any])] = {
        indexFiles.toSeq map { case (indexColumn, indexDevice) =>
          val indexColumnID = getColumnIdByName(indexColumn)
          val oldValue = row_?.flatMap(_.fields(indexColumnID).value)
          val newValue = values.get(indexColumn)
          (indexDevice, oldValue, newValue)
        }
      }
      oldAndNewValues foreach {
        case (indexDevice, oldValue, newValue) => f(indexDevice, oldValue, newValue)
        case _ =>
      }
    }
    result
  }

}

/**
 * Table File Companion
 */
object TableFile {

  /**
   * Retrieves a table by name
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @return the [[TableFile]]
   */
  def apply(databaseName: String, tableName: String): TableFile = {
    val (config, device) = getTableDevice(databaseName, tableName)
    new TableFile(databaseName, tableName, config, device)
  }

  /**
    * Creates a new database table
    * @param databaseName the name of the database
    * @param tableName    the name of the table
    * @param properties   the [[TableProperties table properties]]
    * @return the new [[TableFile]]
    */
  def createTable(databaseName: String, tableName: String, properties: TableProperties): TableFile = {
    val dataFile = getTableDataFile(databaseName, tableName)
    val configFile = getTableConfigFile(databaseName, tableName)
    if (properties.ifNotExists && dataFile.exists() && configFile.exists()) apply(databaseName, tableName)
    else {
      assert(!dataFile.exists(), s"Table '$databaseName.$tableName' already exists")

      // create the root directory
      getTableRootDirectory(databaseName, tableName).mkdirs()

      // create the table configuration file
      val config = TableConfig(columns = properties.columns, properties.isColumnar, indices = Nil, description = properties.description)
      writeTableConfig(databaseName, tableName, config)

      // return the table
      val columns = properties.columns.map(_.toColumn)
      new TableFile(databaseName, tableName, config, new RowOrientedFileBlockDevice(columns, dataFile))
    }
  }

  /**
   * Deletes the table
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param ifExists     indicates whether an existence check before attempting to delete
   * @return true, if the table was deleted
   */
  def dropTable(databaseName: String, tableName: String, ifExists: Boolean = false): Boolean = {
    val dataFile = getViewDataFile(databaseName, tableName)
    val configFile = getTableConfigFile(databaseName, tableName)
    if (!ifExists && !dataFile.exists()) die(s"Table '$tableName' (${dataFile.getAbsolutePath}) does not exist")
    if (!ifExists && !configFile.exists()) die(s"Table '$tableName' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(databaseName, tableName)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

}