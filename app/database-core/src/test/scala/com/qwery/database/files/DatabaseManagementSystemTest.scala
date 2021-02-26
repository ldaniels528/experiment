package com.qwery.database
package files

import com.qwery.database.files.DatabaseManagementSystem._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Database Management System Test
  */
class DatabaseManagementSystemTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(DatabaseManagementSystem.getClass.getSimpleName) {

    it("should retrieve the table summary for a specific database") {
      val databaseSummary = getDatabaseSummary(databaseName = "test")
      assert(databaseSummary.tables.nonEmpty)
      info(f"${"tableName"}%-25s ${"tableType"}%-20s")
      databaseSummary.tables foreach { table =>
        logger.info(f"${table.tableName}%-25s ${table.tableType}%-20s")
      }
    }

    it("should search for databases") {
      val searchResults = searchDatabases(databaseNamePattern = Some("t%"))
      assert(searchResults.nonEmpty)
      logger.info(f"${"databaseName"}%-25s")
      searchResults.zipWithIndex foreach { case (database, index) =>
        logger.info(f"[${index + 1}%02d] ${database.databaseName}")
      }
    }

    it("should search for tables within a database") {
      val searchResults = searchTables(databaseNamePattern = Some("test"), schemaNamePattern = Some("public"), tableNamePattern = Some("%"))
      assert(searchResults.nonEmpty)
      logger.info(f"${"databaseName"}%-25s ${"schemaName"}%-25s ${"tableName"}%-25s")
      searchResults foreach { result =>
        logger.info(f"${result.databaseName}%-25s ${result.schemaName}%-25s ${result.tableName}%-25s")
      }
    }

    it("should search for columns from all tables within a database") {
      val searchResults = searchColumns(databaseNamePattern = Some("test"), schemaNamePattern = Some("stocks"), tableNamePattern = Some("stocks%"), columnNamePattern = Some("symbol"))
      assert(searchResults.nonEmpty)
      logger.info(f"${"databaseName"}%-25s ${"schemaName"}%-25s ${"tableName"}%-25s ${"columnName"}%-25s")
      searchResults foreach { result =>
        logger.info(f"${result.databaseName}%-25s ${result.schemaName}%-25s ${result.tableName}%-25s ${result.column.name}%-25s")
      }
    }

  }

}
