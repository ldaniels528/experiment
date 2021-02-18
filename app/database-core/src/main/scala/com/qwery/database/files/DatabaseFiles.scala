package com.qwery.database.files

import com.qwery.database.JSONSupport._
import com.qwery.database.device.{BlockDevice, ColumnOrientedFileBlockDevice, RowOrientedFileBlockDevice, TableIndexRef}
import com.qwery.database.files.DatabaseFiles.implicits.RichFiles
import com.qwery.database.getServerRootDirectory
import com.qwery.language.SQLDecompiler.implicits.InvokableDeserializer
import com.qwery.language.SQLLanguageParser
import com.qwery.models.Invokable
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

  def getTableConfigFile(databaseName: String, tableName: String): File = {
    getTableRootDirectory(databaseName, tableName) / s"$tableName.json"
  }

  def getTableDevice(databaseName: String, tableName: String): (TableConfig, BlockDevice) = {
    val (configFile, dataFile) = (getTableConfigFile(databaseName, tableName), getTableDataFile(databaseName, tableName))
    assert(configFile.exists() && dataFile.exists(), s"Table '$databaseName.$tableName' does not exist")

    val config = readTableConfig(databaseName, tableName)
    val device = if (config.isColumnar)
      ColumnOrientedFileBlockDevice(columns = config.columns, dataFile)
    else
      new RowOrientedFileBlockDevice(columns = config.columns, dataFile)
    (config, device)
  }

  def getTableRootDirectory(databaseName: String, tableName: String): File = {
    getServerRootDirectory / databaseName / tableName
  }

  def getTableColumnFile(databaseName: String, tableName: String, columnID: Int): File = {
    getTableRootDirectory(databaseName, tableName) / getTableFileName(tableName, columnID)
  }

  def getTableDataFile(databaseName: String, tableName: String): File = {
    getTableRootDirectory(databaseName, tableName) / getTableFileName(tableName)
  }

  def getTableIndices(databaseName: String, tableName: String): Seq[TableIndexRef] = {
    readTableConfig(databaseName, tableName).indices
  }

  def getTableFileName(tableName: String): String = s"$tableName.qdb"

  def getTableFileName(tableName: String, columnID: Int): String = s"$tableName-$columnID.qdb"

  def isTableFile(databaseName: String, tableName: String): Boolean = {
    getTableDataFile(databaseName, tableName).exists()
  }

  def readTableConfig(databaseName: String, tableName: String): TableConfig = {
    Source.fromFile(getTableConfigFile(databaseName, tableName)).use(_.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(databaseName: String, tableName: String, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(databaseName, tableName)).use(_.println(config.toJSONPretty))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIRTUAL TABLE (VIEW) CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getViewDataFile(databaseName: String, viewName: String): File = {
    getTableRootDirectory(databaseName, viewName) / getViewFileName(viewName)
  }

  def getViewFileName(viewName: String): String = s"$viewName.sql"

  def isVirtualTable(databaseName: String, viewName: String): Boolean = {
    getViewDataFile(databaseName, viewName).exists()
  }

  def readViewData(databaseName: String, viewName: String): Invokable = {
    val file = getViewDataFile(databaseName, viewName)
    assert(file.exists(), s"Table '$viewName' does not exist")
    val sql = Source.fromFile(file).use(_.mkString)
    SQLLanguageParser.parse(sql)
  }

  def writeViewData(databaseName: String, viewName: String, query: Invokable): Unit = {
    new PrintWriter(getViewDataFile(databaseName, viewName)).use(_.println(query.toSQL))
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
