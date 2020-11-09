package com.qwery.database

import java.io.{File, PrintWriter}

import com.qwery.util.ResourceHelper._
import com.qwery.database.DatabaseFile.getDatabaseRootDirectory
import com.qwery.database.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.database.models.{DatabaseConfig, DatabaseMetrics}

import scala.io.Source

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
    Source.fromFile(getDatabaseConfigFile(databaseName)).use(src => src.mkString.fromJSON[DatabaseConfig])
  }

  def writeDatabaseConfig(databaseName: String, config: DatabaseConfig): Unit = {
    new PrintWriter(getDatabaseConfigFile(databaseName)).use(_.println(config.toJSONPretty))
  }

}