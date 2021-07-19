package com.qwery.database
package files

import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.TableColumn.implicits._
import com.qwery.database.models.TableConfig
import com.qwery.language.SQLDecompiler.implicits._
import com.qwery.models.{EntityRef, Table}

/**
  * Represents a physical table file
  * @param ref    the [[EntityRef table reference]]
  * @param config the [[TableConfig table configuration]]
  * @param device the [[BlockDevice block device]]
  */
case class TableFile(ref: EntityRef, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * Table File Companion
  */
object TableFile {

  /**
    * Retrieves a table by name
    * @param ref the [[EntityRef]]
    * @return the [[TableFile]]
    */
  def apply(ref: EntityRef): TableFile = {
    val (config, device) = getTableDevice(ref)
    new TableFile(ref, config, device)
  }

  /**
    * Creates a new database table
    * @param table the [[Table table properties]]
    * @return the new [[TableFile]]
    */
  def createTable(table: Table): TableFile = {
    val ref = table.ref
    val configFile = getTableConfigFile(ref)
    val columns = table.columns.map(_.toTableColumn)
    if (table.ifNotExists && configFile.exists()) apply(ref)
    else {
      // create the root directory
      getTableRootDirectory(ref).mkdirs()

      // create the table configuration file
      val config = TableConfig(columns, description = table.description)
      writeTableConfig(ref, config)

      // get a reference to the data file
      val dataFile = getTableDataFile(ref, config)
      assert(!dataFile.exists(), s"Table '${ref.toSQL}' already exists")

      // return the table
      new TableFile(ref, config, new RowOrientedFileBlockDevice(columns, dataFile))
    }
  }

  /**
    * Deletes the table
    * @param ref      the [[EntityRef]]
    * @param ifExists indicates whether an existence check before attempting to delete
    * @return true, if the table was deleted
    */
  def dropTable(ref: EntityRef, ifExists: Boolean = false): Boolean = {
    val configFile = getTableConfigFile(ref)
    if (!ifExists && !configFile.exists()) die(s"Table '${ref.toSQL}' (${configFile.getAbsolutePath}) does not exist")
    val directory = getTableRootDirectory(ref)
    val files = directory.listFilesRecursively
    files.forall(_.delete())
  }

}