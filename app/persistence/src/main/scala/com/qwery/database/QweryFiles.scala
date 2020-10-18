package com.qwery.database

import java.io.{File, PrintWriter}

import com.qwery.database.server.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.database.server.TableService.TableConfig
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.language.postfixOps

/**
 * Qwery Files
 */
object QweryFiles {

  def getDatabaseRootDirectory(databaseName: String): File = {
    new File(getServerRootDirectory, databaseName)
  }

  def getServerRootDirectory: File = {
    val directory = new File(sys.env.getOrElse("QWERY_DB", "qwery_db"))
    assert(directory.mkdirs() || directory.exists(), throw DataDirectoryNotFoundException(directory))
    directory
  }

  def getTableConfigFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.json")
  }

  def getTableRootDirectory(databaseName: String, tableName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), tableName)
  }

  def getTableDataFile(databaseName: String, tableName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$tableName.qdb")
  }

  def getTableIndexFile(databaseName: String, tableName: String, indexName: String): File = {
    new File(new File(new File(getServerRootDirectory, databaseName), tableName), s"$indexName.qdb")
  }

  def readTableConfig(databaseName: String, tableName: String): TableConfig = {
    Source.fromFile(getTableConfigFile(databaseName, tableName)).use(src => src.mkString.fromJSON[TableConfig])
  }

  def writeTableConfig(databaseName: String, tableName: String, config: TableConfig): Unit = {
    new PrintWriter(getTableConfigFile(databaseName, tableName)).use(_.println(config.toJSONPretty))
  }

}
