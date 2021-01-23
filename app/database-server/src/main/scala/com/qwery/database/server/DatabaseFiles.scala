package com.qwery.database
package server

import java.io.{File, PrintWriter}

import com.qwery.database.JSONSupport._
import com.qwery.database.device._
import com.qwery.database.models._
import com.qwery.util.ResourceHelper._

import scala.io.Source

/**
  * Database Files
  */
object DatabaseFiles {

  def getTableDevice(databaseName: String, tableName: String): (TableConfig, BlockDevice) = {
    val (configFile, dataFile) = (getTableConfigFile(databaseName, tableName), getTableDataFile(databaseName, tableName))
    assert(configFile.exists() && dataFile.exists(), s"Table '$databaseName.$tableName' does not exist")

    val config = readTableConfig(databaseName, tableName)
    val device = if (config.isColumnar)
      ColumnOrientedFileBlockDevice(columns = config.columns.map(_.toColumn), dataFile)
    else
      new RowOrientedFileBlockDevice(columns = config.columns.map(_.toColumn), dataFile)
    (config, device)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  DATABASE CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getDatabaseConfigFile(databaseName: String): File = {
    new File(getDatabaseRootDirectory(databaseName), s"$databaseName.json")
  }

  def getDatabaseRootDirectory(databaseName: String): File = {
    new File(getServerRootDirectory, databaseName)
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
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.json")
  }

  def getTableRootDirectory(databaseName: String, tableName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), tableName)
  }

  def getTableColumnFile(databaseName: String, tableName: String, columnID: Int): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName-$columnID.qdb")
  }

  def getTableDataFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.qdb")
  }

  def getTableIndices(databaseName: String, tableName: String): Seq[TableIndexRef] = {
    readTableConfig(databaseName, tableName).indices
  }

  def readTableConfig(databaseName: String, tableName: String): TableConfig = {
    Source.fromFile(getTableConfigFile(databaseName, tableName)).use(_.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(databaseName: String, tableName: String, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(databaseName, tableName)).use(_.println(config.toJSONPretty))
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  VIEW CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getViewConfigFile(databaseName: String, viewName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), s"$viewName.sql")
  }

  def isVirtualTable(databaseName: String, viewName: String): Boolean = {
    getViewConfigFile(databaseName, viewName).exists()
  }

  def readViewConfig(databaseName: String, viewName: String): String = {
    val file = getViewConfigFile(databaseName, viewName)
    if (!file.exists()) die(s"Table '$viewName' does not exist") else Source.fromFile(file).use(_.mkString)
  }

  def writeViewConfig(databaseName: String, viewName: String, queryString: String): Unit = {
    new PrintWriter(getViewConfigFile(databaseName, viewName)).use(_.println(queryString))
  }

}
