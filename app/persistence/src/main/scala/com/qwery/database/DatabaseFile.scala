package com.qwery.database

import java.io.{File, PrintWriter}

import com.qwery.database.DatabaseFile.getDatabaseRootDirectory
import com.qwery.database.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.database.models.{DatabaseConfig, DatabaseInfo, DatabaseMetrics, TableInfo}
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.language.postfixOps

/**
 * Represents a database file
 * @param databaseName the name of the database
 * @param config       the [[DatabaseConfig database configuration]]
 */
case class DatabaseFile(databaseName: String, config: DatabaseConfig) {

  def getDatabaseMetrics: DatabaseMetrics = {
    val directory = getDatabaseRootDirectory(databaseName)
    val tableConfigs = directory.listFilesRecursively.map(_.getName) flatMap {
      case name if name.endsWith(".json") =>
        name.lastIndexOf('.') match {
          case -1 => None
          case index => Some(name.substring(0, index))
        }
      case _ => None
    }
    DatabaseMetrics(databaseName = databaseName, tables = tableConfigs)
  }

}

/**
 * Database File Companion
 */
object DatabaseFile {

  /**
   * Retrieves a database file
   * @param databaseName the name of the database
   */
  def apply(databaseName: String): DatabaseFile = {
    new DatabaseFile(databaseName, config = readDatabaseConfig(databaseName))
  }

  def listColumns(databaseName: String, tableNamePattern: Option[String], columnNamePattern: Option[String]): List[TableInfo] = {
    val directory = getDatabaseRootDirectory(databaseName)
    Option(directory.listFiles()).toList.flatMap(_.toList) flatMap { file =>
      val tableName = file.getName
      val isTableMatch = tableNamePattern.isEmpty || tableNamePattern.exists(_.isEmpty) || tableNamePattern.exists(_.contains(tableName))
      if (isTableMatch && TableFile.getTableConfigFile(databaseName, file.getName).exists()) {
        val config = TableFile.readTableConfig(databaseName, tableName)
        config.columns.collect {
          case column if columnNamePattern.isEmpty || columnNamePattern.exists(_.isEmpty) || columnNamePattern.exists(_.contains(column.name)) =>
            TableInfo(databaseName, tableName, column)
        }
      }
      else Nil
    }
  }

  def listDatabases: List[DatabaseInfo] = {
    getServerRootDirectory.listFiles().toList collect {
      case directory if directory.isDirectory => DatabaseInfo(directory.getName)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  DATABASE CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getDatabaseConfigFile(databaseName: String): File = {
    new File(new File(getServerRootDirectory, databaseName), s"$databaseName.json")
  }

  def getDatabaseRootDirectory(databaseName: String): File = {
    new File(getServerRootDirectory, databaseName)
  }

  def readDatabaseConfig(databaseName: String): DatabaseConfig = {
    val file = getDatabaseConfigFile(databaseName)
    if (file.exists())
      Source.fromFile(file).use(src => src.mkString.fromJSON[DatabaseConfig])
    else
      DatabaseConfig(types = Nil)
  }

  def writeDatabaseConfig(databaseName: String, config: DatabaseConfig): Unit = {
    new PrintWriter(getDatabaseConfigFile(databaseName)).use(_.println(config.toJSONPretty))
  }

}