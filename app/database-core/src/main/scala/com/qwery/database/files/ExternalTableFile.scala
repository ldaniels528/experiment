package com.qwery.database
package files

import com.qwery.database.Column.implicits._
import com.qwery.database.device.ExternalFileBlockDevice
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.TableConfig.ExternalTableConfig
import com.qwery.models.ExternalTable

import java.io.File

/**
  * External Table File
  * @param databaseName the database name
  * @param tableName    the table name
  * @param config       the [[TableConfig table configuration]]
  * @param device       the [[ExternalFileBlockDevice external file block device]]
  */
case class ExternalTableFile(databaseName: String, tableName: String, config: TableConfig, device: ExternalFileBlockDevice) extends TableFileLike

/**
  * External Table File Companion
  */
object ExternalTableFile {

  /**
   * Loads an External Table File
   * @param databaseName the database name
   * @param tableName    the table name
   * @return the [[ExternalTable external table]]
   */
  def apply(databaseName: String, tableName: String): ExternalTableFile = {
    ExternalTableFile(databaseName, tableName, config = readTableConfig(databaseName, tableName))
  }

  /**
   * Loads an External Table File
   * @param databaseName the database name
   * @param tableName    the table name
   * @param config       the [[TableConfig table configuration]]
   * @return the [[ExternalTable external table]]
   */
  def apply(databaseName: String, tableName: String, config: TableConfig): ExternalTableFile = {
    new ExternalTableFile(databaseName, tableName, config, device = ExternalFileBlockDevice(databaseName, tableName, config))
  }

  /**
   * Creates a new external table
   * @param databaseName the database name
   * @param ref          the [[ExternalTable external table]]
   * @return a new [[ExternalTable external table]]
   */
  def createTable(databaseName: String, ref: ExternalTable): ExternalTableFile = {
    val tableName = ref.name

    // create the root directory
    getTableRootDirectory(databaseName, tableName).mkdirs()

    // get a reference to the file or directory
    val rootFile = ref.location map (path => new File(path)) getOrElse die("A Location property was expected")

    // create the table configuration file
    val config = TableConfig(
      columns = ref.columns.map(_.toColumn),
      indices = Nil,
      description = ref.description,
      externalTable = Some(ExternalTableConfig(
        format = ref.format.map(_.toString),
        location = Some(rootFile.getCanonicalPath),
        fieldTerminator = ref.fieldTerminator,
        lineTerminator = ref.lineTerminator,
        headersIncluded = ref.headersIncluded,
        nullValue = ref.nullValue
      ))
    )

    // write the config file
    writeTableConfig(databaseName, tableName, config)

    // return the table
    ExternalTableFile(databaseName, tableName, config)
  }

}