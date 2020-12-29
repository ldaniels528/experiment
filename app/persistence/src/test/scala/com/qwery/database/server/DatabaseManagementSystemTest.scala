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

    it("should list all databases") {
      searchDatabases().zipWithIndex foreach { case (database, index) =>
        logger.info(f"[${index + 1}%02d] ${database.databaseName}")
      }
    }

    it("should list desired columns from all tables within a database") {
      val list = searchColumns(databaseNamePattern = Some("test"), tableNamePattern = None, columnNamePattern = Some("symbol"))
      logger.info(f"${"databaseName"}%-25s ${"tableName"}%-25s ${"columnName"}%-25s")
      list foreach { tableInfo =>
        logger.info(f"${tableInfo.databaseName}%-25s ${tableInfo.tableName}%-25s ${tableInfo.column.name}%-25s")
      }
    }

  }

}
