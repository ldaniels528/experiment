package com.qwery.database.files

import com.qwery.database.DatabaseCPU.toCriteria
import com.qwery.database.device.{BlockDevice, BlockDeviceQuery}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.{KeyValues, RecursiveFileList, createTempTable, die}
import com.qwery.models.expressions.{Condition, Expression, Field => SQLField}
import com.qwery.models.{Invokable, OrderColumn, Select, TableRef}

/**
 * Represents a virtual table file (e.g. view)
 * @param databaseName the name of the database
 * @param viewName     the name of the virtual table
 * @param device       the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(databaseName: String, viewName: String, device: BlockDevice) {
  private val selector = new BlockDeviceQuery(device)

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

  def countRows(condition: KeyValues, limit: Option[Int] = None): Long = device.whileRow(condition, limit) { _ => }

  /**
    * Retrieves rows matching the given condition up to the optional limit
    * @param condition the given [[KeyValues condition]]
    * @param limit     the optional limit
    * @return the [[BlockDevice results]]
    */
  def getRows(condition: KeyValues, limit: Option[Int] = None): BlockDevice = {
    implicit val results: BlockDevice = createTempTable(device.columns)
    device.whileRow(condition, limit) { row =>
      results.writeRow(row.toBinaryRow)
    }
    results
  }

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

}

/**
  * View File Companion
  */
object VirtualTableFile {

  def apply(databaseName: String, viewName: String): VirtualTableFile = {
    new VirtualTableFile(databaseName, viewName, device = getViewDevice(databaseName, viewName))
  }

  def createView(databaseName: String, viewName: String, description: Option[String], invokable: Invokable, ifNotExists: Boolean): VirtualTableFile = {
    val viewFile = getViewDataFile(databaseName, viewName)
    if (viewFile.exists() && !ifNotExists) die(s"View '$viewName' already exists")
    else {
      // create the root directory
      getTableRootDirectory(databaseName, viewName).mkdirs()

      // create the virtual table configuration file
      val config = TableConfig(getProjectionColumns(databaseName, invokable), isColumnar = false, indices = Nil, description)
      writeTableConfig(databaseName, viewName, config)

      // write the virtual table data
      writeViewData(databaseName, viewName, invokable)
    }
    VirtualTableFile(databaseName, viewName)
  }

  def dropView(databaseName: String, viewName: String, ifExists: Boolean): Boolean = {
    val dataFile = getViewDataFile(databaseName, viewName)
    val configFile = getTableConfigFile(databaseName, viewName)
    if (!ifExists && !dataFile.exists()) die(s"View '$viewName' (${dataFile.getAbsolutePath}) does not exist")
    if (!ifExists && !configFile.exists()) die(s"View '$viewName' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(databaseName, viewName)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

  def getViewDevice(databaseName: String, viewName: String): BlockDevice = {
    readViewData(databaseName, viewName) match {
      case Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
        TableFile(databaseName, tableName).selectRows(fields, toCriteria(where), groupBy, having, orderBy, limit)
      case other => die(s"Unhandled view query model - $other")
    }
  }

  private def getProjectionColumns(databaseName: String, invokable: Invokable): Seq[TableColumn] = {
    invokable match {
      case Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
        val tableFile = TableFile(databaseName, tableName)
        val tableQuery = new BlockDeviceQuery(tableFile.device)
        tableQuery.explainColumns(fields).map(_.toTableColumn)
      case unknown => die(s"Unexpected instruction: $unknown")
    }
  }

}