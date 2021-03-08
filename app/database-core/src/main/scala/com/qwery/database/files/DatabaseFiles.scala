package com.qwery.database.files

import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.files.DatabaseFiles.implicits.RichFiles
import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.models.{DatabaseConfig, TableConfig}
import com.qwery.database.util.JSONSupport._
import com.qwery.database.{DEFAULT_DATABASE, DEFAULT_SCHEMA, die, getServerRootDirectory}
import com.qwery.implicits.MagicImplicits
import com.qwery.language.SQLLanguageParser
import com.qwery.models.{EntityRef, Invokable, TableIndex}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._

import java.io._
import scala.io.Source
import scala.util.Try

/**
  * Database Files
  */
object DatabaseFiles {

  //////////////////////////////////////////////////////////////////////////////////////
  //  DATABASE CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getDatabaseConfigFile(databaseName: String): File = {
    getDatabaseRootDirectory(databaseName) / s"$databaseName.json"
  }

  def getDatabaseRootDirectory(databaseName: String): File = {
    getServerRootDirectory / databaseName
  }

  def readDatabaseConfig(databaseName: String): DatabaseConfig = {
    val file = getDatabaseConfigFile(databaseName)
    if (file.exists())
      Source.fromFile(file).use(_.mkString.fromJSON[DatabaseConfig])
    else
      DatabaseConfig(types = Nil)
  }

  def writeDatabaseConfig(databaseName: String, config: DatabaseConfig): Unit = {
    new PrintWriter(getDatabaseConfigFile(databaseName)).use(_.println(config.toJSONPretty))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TABLE CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getTableConfigFile(ref: EntityRef): File = getTableRootDirectory(ref) / s"${ref.name}.json"

  def getTableDevice(ref: EntityRef): (TableConfig, BlockDevice) = {
    // load the table configuration
    val configFile = getTableConfigFile(ref)
    assert(configFile.exists(), s"Table configuration file for '${ref.toSQL}' does not exist [${configFile.getAbsolutePath}]")
    val config = readTableConfig(ref)

    // load the table data file
    val dataFile = getTableDataFile(ref, config)
    assert(dataFile.exists(), s"Table data file for '${ref.toSQL}' does not exist [${dataFile.getAbsolutePath}]")

    // load the table device
    val device = new RowOrientedFileBlockDevice(columns = config.columns, dataFile)
    (config, device)
  }

  def getTableRootDirectory(ref: EntityRef): File = {
    getServerRootDirectory / (ref.databaseName || DEFAULT_DATABASE) / (ref.schemaName || DEFAULT_SCHEMA) / ref.name
  }

  def getTableColumnFile(ref: EntityRef, columnID: Int): File = {
    getTableRootDirectory(ref) / getTableFileName(ref, columnID)
  }

  def getTableDataFile(ref: EntityRef, config: TableConfig): File = {
    config.physicalTable.map(_.location).map(new File(_)) || getTableRootDirectory(ref) / getTableFileName(ref)
  }

  def getTableIndices(ref: EntityRef): Seq[TableIndex] = readTableConfig(ref).indices

  def getTableFileName(ref: EntityRef): String = s"${ref.name}.qdb"

  def getTableFileName(ref: EntityRef, columnID: Int): String = f"${ref.name}.c$columnID%03d.qdb"

  def readTableConfig(ref: EntityRef): TableConfig = {
    Source.fromFile(getTableConfigFile(ref)).use(_.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(ref: EntityRef, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(ref)).use(_.println(config.toJSONPretty))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TABLE INDEX CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getTableIndexFile(tableIndexRef: TableIndex, indexColumnName: String): File = {
    val indexFileName = s"${tableIndexRef.ref.name}_$indexColumnName.qdb"
    val databaseName = tableIndexRef.ref.databaseName || DEFAULT_DATABASE
    val schemaName = tableIndexRef.ref.schemaName || DEFAULT_SCHEMA
    getServerRootDirectory / databaseName / schemaName / tableIndexRef.ref.name / indexFileName
  }

  def getTableIndexFile(tableIndexRef: TableIndex): File = getTableIndexFile(tableIndexRef, tableIndexRef.indexColumnName)

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIRTUAL TABLE (VIEW) CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def isVirtualTable(ref: EntityRef): Boolean = Try(readTableConfig(ref)).toOption.flatMap(_.virtualTable).nonEmpty

  def readViewQuery(ref: EntityRef): Invokable = {
    getTableConfigFile(ref) as { file => assert(file.exists(), s"View '${ref.toSQL}' does not exist [${file.getAbsolutePath}]") }
    val sql = readTableConfig(ref).virtualTable.map(_.queryString).getOrElse(die(s"Object '${ref.toSQL}' is not a view"))
    SQLLanguageParser.parse(sql)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  IMPLICIT DEFINITIONS
  //////////////////////////////////////////////////////////////////////////////////////

  object implicits {

    final implicit class RichFiles(val parentFile: File) extends AnyVal {
      @inline def /(path: String) = new File(parentFile, path)
    }

    final implicit class DBFilesConfig(val config: TableConfig) extends AnyVal {
      @inline def isExternalTable: Boolean = config.externalTable.nonEmpty

      @inline def isVirtualTable: Boolean = config.virtualTable.nonEmpty
    }

  }

}
