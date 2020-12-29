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

  def getDatabaseMetrics(databaseName: String): DatabaseMetrics = {
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

  def searchColumns(databaseNamePattern: Option[String], tableNamePattern: Option[String], columnNamePattern: Option[String]): List[ColumnSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatMap(_.toList)
      databaseName = databaseDirectory.getName if databaseNamePattern isLike databaseName
      tableFile <- Option(databaseDirectory.listFiles()).toList.flatMap(_.toList)
      tableName = tableFile.getName if tableNamePattern isLike tableName
      config <- Try(readTableConfig(databaseName, tableFile.getName)).toOption.toList
      result <- config.columns collect {
        case column if columnNamePattern isLike column.name => ColumnSearchResult(databaseName, tableName, column)
      }
    } yield result
  }

  def searchDatabases(databaseNamePattern: Option[String] = None): List[DatabaseSearchResult] = {
    getServerRootDirectory.listFiles().toList collect {
      case directory if directory.isDirectory && (databaseNamePattern isLike directory.getName) => DatabaseSearchResult(directory.getName)
    }
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
      @inline def isLike(s: String): Boolean = pattern.isEmpty || pattern.map(search).exists(_.matches(s))
    }
  }

}
