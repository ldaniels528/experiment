package com.qwery.database
package files

import com.qwery.database.DatabaseCPU.toCriteria
import com.qwery.database.device.{BlockDevice, BlockDeviceQuery}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.{Column, TableConfig}
import com.qwery.models.{Invokable, Select, TableRef}

/**
  * Represents a virtual table file (e.g. view)
  * @param ref    the [[TableRef table reference]]
  * @param config the [[TableConfig table configuration]]
  * @param device the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(ref: TableRef, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * View File Companion
  */
object VirtualTableFile {

  def load(ref: TableRef): VirtualTableFile = {
    new VirtualTableFile(ref,
      config = readTableConfig(ref),
      device = getViewDevice(ref))
  }

  def createView(ref: TableRef, description: Option[String], invokable: Invokable, ifNotExists: Boolean): VirtualTableFile = {
    val viewFile = getViewDataFile(ref)
    if (viewFile.exists() && !ifNotExists) die(s"View '$ref' already exists")
    else {
      // create the root directory
      getTableRootDirectory(ref).mkdirs()

      // create the virtual table configuration file
      val config = TableConfig(columns = getProjectionColumns(ref, invokable), indices = Nil, description = description)
      writeTableConfig(ref, config)

      // write the virtual table data
      writeViewData(ref, invokable)
    }
    VirtualTableFile.load(ref)
  }

  def dropView(ref: TableRef, ifExists: Boolean): Boolean = {
    val dataFile = getViewDataFile(ref)
    val configFile = getTableConfigFile(ref)
    if (!ifExists && !dataFile.exists()) die(s"View '$ref' (${dataFile.getAbsolutePath}) does not exist")
    if (!ifExists && !configFile.exists()) die(s"View '$ref' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(ref)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

  def getViewDevice(ref: TableRef): BlockDevice = {
    readViewData(ref) match {
      case Select(fields, Some(hostTable: TableRef), joins, groupBy, having, orderBy, where, limit) =>
        TableFile(hostTable).selectRows(fields, toCriteria(where), groupBy, having, orderBy, limit)
      case other => die(s"Unhandled view query model - $other")
    }
  }

  private def getProjectionColumns(ref: TableRef, invokable: Invokable): Seq[Column] = {
    invokable match {
      case Select(fields, Some(tableRef: TableRef), joins, groupBy, having, orderBy, where, limit) =>
        val tableFile = TableFile(tableRef)
        val tableQuery = new BlockDeviceQuery(tableFile.device)
        tableQuery.explainColumns(fields)
      case unknown => die(s"Unexpected instruction: $unknown")
    }
  }

}