package com.qwery.database
package files

import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.DatabaseManagementSystem.implicits._
import com.qwery.database.models._
import com.qwery.models.TableRef

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Date, TimeZone}
import scala.util.Try

/**
  * Database Management System
  */
object DatabaseManagementSystem {
  val COLUMNAR_TABLE = "COLUMNAR_TABLE"
  val LOGICAL_TABLE = "LOGICAL_TABLE"
  val TABLE = "TABLE"
  val VIEW = "VIEW"
  val tableTypes = Seq(COLUMNAR_TABLE, LOGICAL_TABLE, TABLE, VIEW)

  /**
    * Retrieves the summary for a database by name
    * @param databaseName the database name
    * @return the [[DatabaseSummary database summary]]
    */
  def getDatabaseSummary(databaseName: String, schemaNamePattern: Option[String] = None): DatabaseSummary = {
    val databaseDirectory = getDatabaseRootDirectory(databaseName)
    val schemaDirectories = Option(databaseDirectory.listFiles).toList.flatten
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    sdf.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")))
    DatabaseSummary(databaseName, tables = schemaDirectories.flatMap {
      case schemaDirectory if schemaDirectory.isDirectory & !schemaDirectory.isHidden & (schemaNamePattern like schemaDirectory.getName) =>
        val schemaName = schemaDirectory.getName

        // get the tables inside of the schema
        val tableDirectories = Option(schemaDirectory.listFiles).toList.flatten
        tableDirectories.flatMap {
          case tableDirectory if tableDirectory.isDirectory & !tableDirectory.isHidden =>
            val tableName = tableDirectory.getName
            val table = new TableRef(databaseName, schemaName, tableName)

            // is it a physical table?
            if (isTableFile(table)) {
              Try(readTableConfig(table)).toOption map { config =>
                val dataFile = getTableDataFile(table)
                val modifiedTime = sdf.format(new Date(dataFile.lastModified()))
                val tableType =
                  if (config.isColumnar) COLUMNAR_TABLE
                  else if (config.externalTable.nonEmpty) LOGICAL_TABLE
                  else TABLE
                TableSummary(tableName, schemaName, tableType, description = config.description, lastModifiedTime = modifiedTime, fileSize = dataFile.length())
              }
            }
            // is it a view table?
            else if (isVirtualTable(table)) {
              Try(readTableConfig(table)).toOption map { config =>
                val dataFile = getViewDataFile(table)
                val modifiedTime = sdf.format(new Date(dataFile.lastModified()))
                TableSummary(tableName, schemaName, tableType = VIEW, description = config.description, lastModifiedTime = modifiedTime, fileSize = dataFile.length())
              }
            }
            // unsupported file
            else None
        }
      case _ => None
    })
  }

  /**
    * Searches for columns by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @param tableNamePattern    the table name search pattern (e.g. "%stocks")
    * @param schemaNamePattern   the schema name search pattern (e.g. "%public")
    * @param columnNamePattern   the column name search pattern (e.g. "%symbol%")
    * @return the promise of a collection of [[ColumnSearchResult search results]]
    */
  def searchColumns(databaseNamePattern: Option[String], schemaNamePattern: Option[String], tableNamePattern: Option[String], columnNamePattern: Option[String]): List[ColumnSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      schemaDirectory <- Option(databaseDirectory.listFiles()).toList.flatten
      schemaName = schemaDirectory.getName if schemaNamePattern like schemaName
      tableFile <- Option(schemaDirectory.listFiles()).toList.flatten
      tableName = tableFile.getName if tableNamePattern like tableName
      config <- Try(readTableConfig(new TableRef(databaseName, schemaName, tableFile.getName))).toOption.toList
      result <- config.columns collect {
        case column if columnNamePattern like column.name => ColumnSearchResult(databaseName, schemaName, tableName, column)
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
    * Searches for schemas by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @param schemaNamePattern   the schema name search pattern (e.g. "%public")
    * @return the promise of a collection of [[SchemaSearchResult search results]]
    */
  def searchSchemas(databaseNamePattern: Option[String], schemaNamePattern: Option[String]): List[SchemaSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      schemaDirectory <- Option(databaseDirectory.listFiles()).toList.flatten
      schemaName = schemaDirectory.getName if schemaNamePattern like schemaName
    } yield SchemaSearchResult(databaseName, schemaName)
  }

  /**
    * Searches for tables by name
    * @param databaseNamePattern the database name search pattern (e.g. "te%t")
    * @param schemaNamePattern   the schema name search pattern (e.g. "%public")
    * @param tableNamePattern    the table name search pattern (e.g. "%stocks")
    * @return the promise of a collection of [[TableSearchResult search results]]
    */
  def searchTables(databaseNamePattern: Option[String], schemaNamePattern: Option[String], tableNamePattern: Option[String]): List[TableSearchResult] = {
    for {
      databaseDirectory <- Option(getServerRootDirectory.listFiles()).toList.flatten
      databaseName = databaseDirectory.getName if databaseNamePattern like databaseName
      schemaDirectory <- Option(databaseDirectory.listFiles()).toList.flatten
      schemaName = schemaDirectory.getName if schemaNamePattern like schemaName
      tableSummary <- getDatabaseSummary(databaseName, schemaNamePattern).tables
      tableName = tableSummary.tableName if tableNamePattern like tableName
    } yield TableSearchResult(databaseName, schemaName, tableName, tableSummary.tableType, tableSummary.description)
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
