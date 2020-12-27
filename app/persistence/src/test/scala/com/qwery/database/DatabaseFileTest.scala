package com.qwery.database

import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Database File Test
  */
class DatabaseFileTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe("DatabaseFile") {

    it("should list all databases") {
      DatabaseFile.listDatabases.zipWithIndex foreach { case (database, index) =>
        logger.info(f"[${index + 1}%02d] ${database.databaseName}")
      }
    }

    it("should list desired columns from all tables within a database") {
      val list = DatabaseFile.listColumns(databaseName = "qwery", tableNamePattern = None, columnNamePattern = Some("symbol"))
      logger.info(f"${"databaseName"}%-25s ${"tableName"}%-25s ${"columnName"}%-25s")
      list foreach { tableInfo =>
        logger.info(f"${tableInfo.databaseName}%-25s ${tableInfo.tableName}%-25s ${tableInfo.column.name}%-25s")
      }
    }

  }

}
