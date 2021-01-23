package com.qwery.database
package server

import com.qwery.database.server.DatabaseFiles._
import com.qwery.database.server.DatabaseManagementSystem.implicits._
import com.qwery.database.models._

import scala.util.Try

/**
  * Database Management System
  */
object DatabaseManagementSystem {
  val tableTypes = Seq("COLUMNAR_TABLE", "TABLE", "VIEW")

  /**
    * Retrieves the summary for a database by name
    * @param databaseName the database name
    * @return the [[DatabaseSummary database summary]]
    */
  def getDatabaseSummary(databaseName: String): DatabaseSummary = {
    val databaseDirectory = getDatabaseRootDirectory(databaseName)
    val tableDirectories = Option(databaseDirectory.listFiles).toList.flatten
    DatabaseSummary(databaseName, tables = tableDirectories.flatMap {
      case tableDirectory if tableDirectory.isDirectory =>
        val tableName = tableDirectory.getName
        Try(DatabaseFiles.readTableConfig(databaseName, tableName)).toOption map { config =>
          val tableType = if (config.isColumnar) "COLUMNAR_TABLE" else "TABLE"
          TableSummary(tableName = tableName, tableType = tableType, description = config.description)
        }
      case viewFile if viewFile.isFile & viewFile.getName.endsWith(".sql") =>
        val viewName = viewFile.getName.substring(0, viewFile.getName.lastIndexOf('.'))
        Some(TableSummary(tableName = viewName, tableType = "VIEW", description = Some("Logical table/view")))
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
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      tableFile <- Option(databaseDirectory.listFiles()).toList.flatten
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
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
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
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      tableSummary <- getDatabaseSummary(databaseName).tables
      tableName = tableSummary.tableName if tableNamePattern like tableName
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
