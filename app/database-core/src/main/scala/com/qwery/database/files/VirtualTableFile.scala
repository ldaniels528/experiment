package com.qwery.database
package files

import com.qwery.database.DatabaseCPU.toCriteria
import com.qwery.database.device.{BlockDevice, BlockDeviceQuery}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.{Column, TableConfig}
import com.qwery.models.{EntityRef, Invokable, Select, View}

/**
  * Represents a virtual table file (e.g. view)
  * @param ref    the [[EntityRef table reference]]
  * @param config the [[TableConfig table configuration]]
  * @param device the [[BlockDevice materialized device]]
  */
case class VirtualTableFile(ref: EntityRef, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * Virtual Table File Companion
  */
object VirtualTableFile {

  /**
   * Retrieves a virtual table by name
   * @param ref the [[EntityRef]]
   * @return the [[VirtualTableFile virtual table]]
   */
  def load(ref: EntityRef): VirtualTableFile = {
    new VirtualTableFile(ref, config = readTableConfig(ref), device = getViewDevice(ref))
  }

  /**
   * Creates a new virtual table
   * @param view the [[View virtual table]]
   * @return a new [[VirtualTableFile virtual table]]
   */
  def createView(view: View): VirtualTableFile = {
    val ref = view.ref
    val viewFile = getViewDataFile(ref)
    if (viewFile.exists() && !view.ifNotExists) die(s"View '$ref' already exists")
    else {
      // create the root directory
      getTableRootDirectory(ref).mkdirs()

      // create the virtual table configuration file
      val config = TableConfig(columns = getProjectionColumns(ref, view.query), indices = Nil, description = view.description)
      writeTableConfig(ref, config)

      // write the virtual table data
      writeViewData(ref, view.query)
    }
    VirtualTableFile.load(ref)
  }

  def dropView(ref: EntityRef, ifExists: Boolean): Boolean = {
    val dataFile = getViewDataFile(ref)
    val configFile = getTableConfigFile(ref)
    if (!ifExists && !dataFile.exists()) die(s"View '$ref' (${dataFile.getAbsolutePath}) does not exist")
    if (!ifExists && !configFile.exists()) die(s"View '$ref' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(ref)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

  def getViewDevice(ref: EntityRef): BlockDevice = {
    readViewData(ref) match {
      case Select(fields, Some(hostTable: EntityRef), joins, groupBy, having, orderBy, where, limit) =>
        TableFile(hostTable).selectRows(fields, toCriteria(where), groupBy, having, orderBy, limit)
      case other => die(s"Unhandled view query model - $other")
    }
  }

  private def getProjectionColumns(ref: EntityRef, invokable: Invokable): Seq[Column] = {
    invokable match {
      case Select(fields, Some(tableRef: EntityRef), joins, groupBy, having, orderBy, where, limit) =>
        val tableFile = TableFile(tableRef)
        val tableQuery = new BlockDeviceQuery(tableFile.device)
        tableQuery.explainColumns(fields)
      case unknown => die(s"Unexpected instruction: $unknown")
    }
  }

}