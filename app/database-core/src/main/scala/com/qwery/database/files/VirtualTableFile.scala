package com.qwery.database
package files

import com.qwery.database.DatabaseCPU.toCriteria
import com.qwery.database.device.{BlockDevice, BlockDeviceQuery}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.models.{Invokable, Select, TableRef}

/**
  * Represents a virtual table file (e.g. view)
  * @param databaseName the name of the database
  * @param tableName    the name of the virtual table
  * @param config       the [[TableConfig table configuration]]
  * @param device       the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(databaseName: String, tableName: String, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * View File Companion
  */
object VirtualTableFile {

  def apply(databaseName: String, viewName: String): VirtualTableFile = {
    new VirtualTableFile(databaseName, viewName,
      config = readTableConfig(databaseName, viewName),
      device = getViewDevice(databaseName, viewName))
  }

  def createView(databaseName: String, viewName: String, description: Option[String], invokable: Invokable, ifNotExists: Boolean): VirtualTableFile = {
    val viewFile = getViewDataFile(databaseName, viewName)
    if (viewFile.exists() && !ifNotExists) die(s"View '$viewName' already exists")
    else {
      // create the root directory
      getTableRootDirectory(databaseName, viewName).mkdirs()

      // create the virtual table configuration file
      val config = TableConfig(columns = getProjectionColumns(databaseName, invokable), isColumnar = false, indices = Nil, description = description)
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

  private def getProjectionColumns(databaseName: String, invokable: Invokable): Seq[Column] = {
    invokable match {
      case Select(fields, Some(TableRef(tableName)), joins, groupBy, having, orderBy, where, limit) =>
        val tableFile = TableFile(databaseName, tableName)
        val tableQuery = new BlockDeviceQuery(tableFile.device)
        tableQuery.explainColumns(fields)
      case unknown => die(s"Unexpected instruction: $unknown")
    }
  }

}