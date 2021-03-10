package com.qwery.database
package files

import com.qwery.database.device.{BlockDevice, ExternalFileBlockDevice}
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.models.TableColumn.implicits._
import com.qwery.database.models.TableConfig
import com.qwery.database.models.TableConfig.ExternalTableConfig
import com.qwery.models.{EntityRef, ExternalTable}

import java.io.File

/**
  * External Table File
  * @param ref    the [[EntityRef table reference]]
  * @param config the [[TableConfig table configuration]]
  * @param device the [[BlockDevice external file block device]]
  */
case class ExternalTableFile(ref: EntityRef, config: TableConfig, device: BlockDevice) extends TableFileLike

/**
  * External Table File Companion
  */
object ExternalTableFile {

  /**
    * Loads an External Table File
    * @param ref the [[EntityRef table reference]]
    * @return the [[ExternalTable external table]]
    */
  def apply(ref: EntityRef): ExternalTableFile = {
    ExternalTableFile(ref, config = readTableConfig(ref))
  }

  /**
    * Loads an External Table File
    * @param ref    the [[EntityRef table reference]]
    * @param config the [[TableConfig table configuration]]
    * @return the [[ExternalTable external table]]
    */
  def apply(ref: EntityRef, config: TableConfig): ExternalTableFile = {
    new ExternalTableFile(ref, config, device = ExternalFileBlockDevice(ref, config))
  }

  /**
    * Creates a new external table
    * @param table the [[ExternalTable external table]]
    * @return a new [[ExternalTable external table]]
    */
  def createTable(table: ExternalTable): ExternalTableFile = {
    // create the root directory
    getTableRootDirectory(table.ref).mkdirs()

    // get a reference to the file or directory
    val rootFile = table.location map (path => new File(path)) getOrElse die("A table reference property was expected")

    // create the table configuration file
    val config = TableConfig(
      columns = table.columns.map(_.toTableColumn),
      indices = Nil,
      description = table.description,
      externalTable = Some(ExternalTableConfig(
        format = table.format.map(_.toLowerCase),
        location = Some(rootFile.getCanonicalPath),
        fieldTerminator = table.fieldTerminator,
        lineTerminator = table.lineTerminator,
        headersIncluded = table.headersIncluded,
        nullValue = table.nullValue
      ))
    )

    // write the config file
    writeTableConfig(table.ref, config)

    // return the table
    ExternalTableFile(table.ref, config)
  }

}