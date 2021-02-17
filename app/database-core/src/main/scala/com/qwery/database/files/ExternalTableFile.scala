package com.qwery.database
package files

import com.qwery.database.Column.implicits._
import com.qwery.database.device.{BlockDevice, ExternalFileBlockDevice}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.TableColumn._
import com.qwery.database.files.TableConfig.ExternalTableConfig
import com.qwery.models.ExternalTable

import java.io.File

/**
  * External Table File
  * @param databaseName the database name
  * @param tableName    the table name
  * @param config       the [[TableConfig table configuration]]
  */
case class ExternalTableFile(databaseName: String, tableName: String, config: TableConfig) extends TableFileLike {

  val device: BlockDevice = ExternalFileBlockDevice(databaseName, tableName, config)

}

/**
  * External Table File Companion
  */
object ExternalTableFile {

  def apply(databaseName: String, tableName: String): ExternalTableFile = {
    new ExternalTableFile(databaseName, tableName, config = readTableConfig(databaseName, tableName))
  }

  def createTable(databaseName: String, ref: ExternalTable): ExternalTableFile = {
    val tableName = ref.name
    val configFile = getTableConfigFile(databaseName, tableName)
    if (configFile.exists()) apply(databaseName, tableName)
    else {
      assert(!configFile.exists(), s"External Table '$databaseName.$tableName' already exists")

      // create the root directory
      getTableRootDirectory(databaseName, tableName).mkdirs()

      // get a reference to the file or directory
      val rootFile = ref.location map(path => new File(path)) getOrElse die("A Location property was expected")

      // create the table configuration file
      val config = TableConfig(
        columns = ref.columns.map(_.toColumn.toTableColumn),
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
      new ExternalTableFile(databaseName, tableName, config)
    }
  }

}