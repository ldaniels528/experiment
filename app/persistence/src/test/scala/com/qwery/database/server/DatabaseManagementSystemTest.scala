package com.qwery.database.server

import com.qwery.database.server.DatabaseManagementSystem._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Database Management System Test
  */
class DatabaseManagementSystemTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(DatabaseManagementSystem.getClass.getSimpleName) {

    it("should search for databases") {
      val searchResults = searchDatabases(databaseNamePattern = Some("t%"))
      logger.info(f"${"databaseName"}%-25s")
      searchResults.zipWithIndex foreach { case (database, index) =>
        logger.info(f"[${index + 1}%02d] ${database.databaseName}")
      }
    }

    it("should search for tables within a database") {
      val searchResults = searchTables(databaseNamePattern = Some("test"), tableNamePattern = Some("stock%"))
      logger.info(f"${"databaseName"}%-25s ${"tableName"}%-25s")
      searchResults foreach { result =>
        logger.info(f"${result.databaseName}%-25s ${result.tableName}%-25s")
      }
    }

    it("should search for columns from all tables within a database") {
      val searchResults = searchColumns(databaseNamePattern = Some("test"), tableNamePattern = None, columnNamePattern = Some("symbol"))
      logger.info(f"${"databaseName"}%-25s ${"tableName"}%-25s ${"columnName"}%-25s")
      searchResults foreach { tableInfo =>
        logger.info(f"${tableInfo.databaseName}%-25s ${tableInfo.tableName}%-25s ${tableInfo.column.name}%-25s")
      }
    }

  }

}
