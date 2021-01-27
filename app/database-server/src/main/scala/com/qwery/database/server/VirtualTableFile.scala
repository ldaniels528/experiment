package com.qwery.database
package server

import com.qwery.database.device.BlockDevice
import com.qwery.database.models.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.models.{TableColumn, TableConfig}
import com.qwery.database.server.DatabaseFiles._
import com.qwery.database.server.QueryProcessor.commands.SelectRows
import com.qwery.database.server.SQLCompiler.implicits.InvokableFacade
import com.qwery.models.expressions.{Expression, Field => SQLField}
import com.qwery.models.{Invokable, OrderColumn}

/**
 * Represents a virtual table file (e.g. view)
 * @param databaseName the name of the database
 * @param viewName     the name of the virtual table
 * @param device       the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(databaseName: String, viewName: String, device: BlockDevice) {
  private val selector = new TableQuery(device)

  /**
   * Closes the underlying file handle
   */
  def close(): Unit = device.close()

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
                 orderBy: Seq[OrderColumn] = Nil,
                 limit: Option[Int] = None): BlockDevice = {
    selector.select(fields, where, groupBy, orderBy, limit)
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
    dataFile.exists() && configFile.exists() && getViewDataFile(databaseName, viewName).delete()
  }

  def getViewDevice(databaseName: String, viewName: String): BlockDevice = {
    import com.qwery.database.server.SQLCompiler.implicits.InvokableFacade
    readViewData(databaseName, viewName).compile(databaseName) match {
      case SelectRows(_, tableName, fields, where, groupBy, orderBy, limit) =>
        TableFile(databaseName, tableName).selectRows(fields, where, groupBy, orderBy, limit)
      case other => die(s"Unhandled view query model - $other")
    }
  }

  private def getProjectionColumns(databaseName: String, invokable: Invokable): Seq[TableColumn] = {
    invokable.compile(databaseName) match {
      case select: SelectRows =>
        val tableFile = TableFile(select.databaseName, select.tableName)
        val tableQuery = new TableQuery(tableFile.device)
        tableQuery.explainColumns(select.fields).map(_.toTableColumn)
      case unknown => die(s"Unexpected instruction: $unknown")
    }
  }

}