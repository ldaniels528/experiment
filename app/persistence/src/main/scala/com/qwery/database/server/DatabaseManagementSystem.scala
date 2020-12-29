package com.qwery.database
package server

import com.qwery.database.DatabaseFiles._
import com.qwery.database.server.DatabaseManagementSystem.implicits._
import com.qwery.database.server.models._

import scala.util.Try

/**
  * Database Management System
  */
object DatabaseManagementSystem {

  /**
    * Retrieves metrics for a database by name
    * @param databaseName the database name
    * @return the [[DatabaseMetrics metrics]]
    */
  def getDatabaseMetrics(databaseName: String): DatabaseMetrics = {
    DatabaseMetrics(databaseName, tables = getDatabaseRootDirectory(databaseName).listFilesRecursively.map(_.getName) flatMap {
      case filename if filename.endsWith(".json") =>
        filename.lastIndexOf('.') match {
          case -1 => None
          case index => Some(filename.substring(0, index))
        }
      case _ => None
    })
  }

  /**
    * Searches for columns by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @param tableNamePattern    the table name search pattern (e.g. "%stocks")
    * @param columnNamePattern   the column name search pattern (e.g. "%symbol%")
    * @return the promise of a collection of [[ColumnSearchResult search results]]
    */
  def searchColumns(databaseNamePattern: Option[String], tableNamePattern: Option[String], columnNamePattern: Option[String]): List[ColumnSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatMap(_.toList)
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      tableFile <- Option(databaseDirectory.listFiles()).toList.flatMap(_.toList)
      tableName = tableFile.getName if tableNamePattern like tableName
      config <- Try(readTableConfig(databaseName, tableFile.getName)).toOption.toList
      result <- config.columns collect {
        case column if columnNamePattern like column.name => ColumnSearchResult(databaseName, tableName, column)
      }
    } yield result
  }

  /**
    * Searches for databases by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @return the promise of a collection of [[DatabaseSearchResult search results]]
    */
  def searchDatabases(databaseNamePattern: Option[String] = None): List[DatabaseSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatMap(_.toList)
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
    } yield DatabaseSearchResult(databaseName)
  }

  /**
    * Searches for tables by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @param tableNamePattern    the table name search pattern (e.g. "%stocks")
    * @return the promise of a collection of [[TableSearchResult search results]]
    */
  def searchTables(databaseNamePattern: Option[String], tableNamePattern: Option[String]): List[TableSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatMap(_.toList)
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      tableFile <- Option(databaseDirectory.listFiles()).toList.flatMap(_.toList)
      tableName = tableFile.getName if tableNamePattern like tableName
    } yield TableSearchResult(databaseName, tableName)
  }

  /**
    * Implicit definitions
    */
  object implicits {
    private val search: String => String = _.replaceAllLiterally("%", ".*")

    /**
      * Pattern Search With Options
      * @param pattern the SQL-like pattern (e.g. "test%")
      */
    final implicit class PatternSearchWithOptions(val pattern: Option[String]) extends AnyVal {
      @inline def like(text: String): Boolean = pattern.isEmpty || pattern.map(search).exists(text.matches)
    }
  }

}
