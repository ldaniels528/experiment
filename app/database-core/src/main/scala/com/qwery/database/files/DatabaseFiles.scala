package com.qwery.database.files

import com.qwery.database.device.{BlockDevice, ColumnOrientedFileBlockDevice, RowOrientedFileBlockDevice}
import com.qwery.database.files.DatabaseFiles.implicits.RichFiles
import com.qwery.database.{DEFAULT_DATABASE, DEFAULT_SCHEMA, getServerRootDirectory}
import com.qwery.database.models.{DatabaseConfig, TableConfig, TableIndexRef}
import com.qwery.database.util.JSONSupport._
import com.qwery.language.SQLDecompiler.implicits.InvokableDeserializer
import com.qwery.language.SQLLanguageParser
import com.qwery.models.{Invokable, TableRef}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._

import java.io._
import scala.io.Source

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

  def getTableConfigFile(ref: TableRef): File = {
    getTableRootDirectory(ref) / s"${ref.tableName}.json"
  }

  def getTableDevice(ref: TableRef): (TableConfig, BlockDevice) = {
    val (configFile, dataFile) = (getTableConfigFile(ref), getTableDataFile(ref))
    assert(configFile.exists() && dataFile.exists(), s"Table '$ref' does not exist")

    val config = readTableConfig(ref)
    val device = if (config.isColumnar)
      ColumnOrientedFileBlockDevice(columns = config.columns, dataFile)
    else
      new RowOrientedFileBlockDevice(columns = config.columns, dataFile)
    (config, device)
  }

  def getTableRootDirectory(ref: TableRef): File = {
    getServerRootDirectory / (ref.databaseName || DEFAULT_DATABASE) / (ref.schemaName || DEFAULT_SCHEMA) / ref.tableName
  }

  def getTableColumnFile(ref: TableRef, columnID: Int): File = {
    getTableRootDirectory(ref) / getTableFileName(ref.tableName, columnID)
  }

  def getTableDataFile(ref: TableRef): File = {
    getTableRootDirectory(ref) / getTableFileName(ref.tableName)
  }

  def getTableIndices(ref: TableRef): Seq[TableIndexRef] = {
    readTableConfig(ref).indices
  }

  def getTableFileName(tableName: String): String = s"$tableName.qdb"

  def getTableFileName(tableName: String, columnID: Int): String = s"$tableName-$columnID.qdb"

  def isTableFile(ref: TableRef): Boolean = {
    getTableDataFile(ref).exists()
  }

  def readTableConfig(ref: TableRef): TableConfig = {
    Source.fromFile(getTableConfigFile(ref)).use(_.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(ref: TableRef, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(ref)).use(_.println(config.toJSONPretty))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIRTUAL TABLE (VIEW) CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getViewDataFile(ref: TableRef): File = {
    getTableRootDirectory(ref) / getViewFileName(ref.tableName)
  }

  def getViewFileName(viewName: String): String = s"$viewName.sql"

  def isVirtualTable(ref: TableRef): Boolean = {
    getViewDataFile(ref).exists()
  }

  def readViewData(ref: TableRef): Invokable = {
    val file = getViewDataFile(ref)
    assert(file.exists(), s"Table '$ref' does not exist")
    val sql = Source.fromFile(file).use(_.mkString)
    SQLLanguageParser.parse(sql)
  }

  def writeViewData(ref: TableRef, query: Invokable): Unit = {
    new PrintWriter(getViewDataFile(ref)).use(_.println(query.toSQL))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  IMPLICIT DEFINITIONS
  //////////////////////////////////////////////////////////////////////////////////////

  object implicits {
    final implicit class RichFiles(val parentFile: File) extends AnyVal {
      @inline def /(path: String) = new File(parentFile, path)
    }
  }

}
