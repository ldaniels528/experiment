package com.qwery.database
package files

import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.files.DatabaseFiles._

/**
 * Represents a physical table file
 * @param databaseName the name of the database
 * @param tableName    the name of the table
 * @param config       the [[TableConfig table configuration]]
 * @param device       the [[BlockDevice block device]]
 */
case class TableFile(databaseName: String, tableName: String, config: TableConfig, device: BlockDevice) extends TableFileLike

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
    * @param ref          the [[TableProperties table properties]]
    * @return the new [[TableFile]]
    */
  def createTable(databaseName: String, tableName: String, ref: TableProperties): TableFile = {
    val dataFile = getTableDataFile(databaseName, tableName)
    val configFile = getTableConfigFile(databaseName, tableName)
    if (ref.ifNotExists && dataFile.exists() && configFile.exists()) apply(databaseName, tableName)
    else {
      assert(!dataFile.exists(), s"Table '$databaseName.$tableName' already exists")

      // create the root directory
      getTableRootDirectory(databaseName, tableName).mkdirs()

      // create the table configuration file
      val config = TableConfig(columns = ref.columns, ref.isColumnar, indices = Nil, description = ref.description)
      writeTableConfig(databaseName, tableName, config)

      // return the table
      new TableFile(databaseName, tableName, config, new RowOrientedFileBlockDevice(ref.columns, dataFile))
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