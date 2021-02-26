package com.qwery.database
package files

import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.Column.implicits._
import com.qwery.database.models.TableConfig
import com.qwery.models.{Table, TableRef}

/**
  * Represents a physical table file
  * @param ref    the [[TableRef table reference]]
  * @param config the [[TableConfig table configuration]]
  * @param device the [[BlockDevice block device]]
  */
case class TableFile(ref: TableRef, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * Table File Companion
  */
object TableFile {

  /**
    * Retrieves a table by name
    * @param ref the [[TableRef]]
    * @return the [[TableFile]]
    */
  def apply(ref: TableRef): TableFile = {
    val (config, device) = getTableDevice(ref)
    new TableFile(ref, config, device)
  }

  /**
    * Creates a new database table
    * @param ref   the [[TableRef]]
    * @param props the [[Table table properties]]
    * @return the new [[TableFile]]
    */
  def createTable(databaseName: String, table: Table): TableFile = {
    val dataFile = getTableDataFile(table.ref)
    val configFile = getTableConfigFile(table.ref)
    val columns = table.columns.map(_.toColumn)
    if (table.ifNotExists && dataFile.exists() && configFile.exists()) apply(table.ref)
    else {
      assert(!dataFile.exists(), s"Table '${table.ref}' already exists")

      // create the root directory
      getTableRootDirectory(table.ref).mkdirs()

      // create the table configuration file
      val config = TableConfig(columns, table.isColumnar, indices = Nil, description = table.description)
      writeTableConfig(table.ref, config)

      // return the table
      new TableFile(table.ref, config, new RowOrientedFileBlockDevice(columns, dataFile))
    }
  }

  /**
    * Deletes the table
    * @param ref      the [[TableRef]]
    * @param ifExists indicates whether an existence check before attempting to delete
    * @return true, if the table was deleted
    */
  def dropTable(ref: TableRef, ifExists: Boolean = false): Boolean = {
    val dataFile = getViewDataFile(ref)
    val configFile = getTableConfigFile(ref)
    if (!ifExists && !dataFile.exists()) die(s"Table '$ref' (${dataFile.getAbsolutePath}) does not exist")
    if (!ifExists && !configFile.exists()) die(s"Table '$ref' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(ref)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

}