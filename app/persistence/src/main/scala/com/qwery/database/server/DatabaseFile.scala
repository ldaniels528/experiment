package com.qwery.database.server

import com.qwery.database.QweryFiles
import com.qwery.database.QweryFiles.getDatabaseRootDirectory
import com.qwery.database.models.{DatabaseConfig, DatabaseMetrics}

/**
 * Represents a database file
 * @param databaseName the name of the database
 * @param config       the [[DatabaseConfig database configuration]]
 */
class DatabaseFile(databaseName: String, config: DatabaseConfig) {

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
    QweryFiles.getDatabaseConfigFile(databaseName)
    val config = DatabaseConfig(types = Nil) // TODO load the config
    new DatabaseFile(databaseName, config)
  }

}