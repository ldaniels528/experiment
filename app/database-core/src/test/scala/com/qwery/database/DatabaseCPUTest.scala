package com.qwery.database

import com.qwery.database.StockQuote._
import com.qwery.database.files.DatabaseFiles
import com.qwery.models.expressions.{Expression, Literal}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Database CPU Test Suite
  */
class DatabaseCPUTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val insertCount = 1e+5.toInt
  private val cpu = new DatabaseCPU()
  private val databaseName = "test"
  private val tableName = "stocks_cpu_test"
  private val viewName = "tickers_NYSE"

  private val newQuote: () => Seq[(String, Expression)] = { () =>
    Seq("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastTradeTime" -> randomDate)
      .map { case (name, value) => name -> Literal(value) }
  }

  describe(classOf[DatabaseCPU].getSimpleName) {

    it("should DROP the existing TABLE") {
      cpu.dropTable(databaseName, tableName, ifExists = true)
      assert(!DatabaseFiles.isTableFile(databaseName, tableName))
    }

    it("should CREATE a new TABLE") {
      val solution = cpu.executeQuery(databaseName, sql =
        s"""|CREATE TABLE IF NOT EXISTS $tableName (
            |   symbol VARCHAR(8) COMMENT "the ticker symbol",
            |   exchange VARCHAR(8) COMMENT "the stock exchange",
            |   lastSale DOUBLE COMMENT "the latest sale price",
            |   lastTradeTime DATETIME COMMENT "the latest sale date/time"
            |) WITH DESCRIPTION 'SQL created table'
            |""".stripMargin)
      assert(solution.get == Right(1L))
    }

    it("should TRUNCATE the existing TABLE") {
       cpu.truncateTable(databaseName, tableName)
    }

    it("should INSERT rows into the TABLE") {
      val clock = stopWatch
      val responses = (1 to insertCount).map(_ => cpu.insertRow(databaseName, tableName, newQuote()))
      val result = cpu.getRow(databaseName, tableName, insertCount / 2)
      val elapsedTime = clock()
      logger.info(f"Inserted ${responses.length} records in $elapsedTime%.1f msec")
      logger.info(f"Retrieved row $result")
      responses.take(20).zipWithIndex foreach { case (response, n) => logger.info(f"insert[$n%03d]: $response") }
    }

    it("should COUNT the rows in the TABLE") {
      val solution = cpu.executeQuery(databaseName, s"SELECT COUNT(*) FROM $tableName")
      assert(solution.get.isLeft)
    }

    it("should perform transformation queries") {
      val solution = cpu.executeQuery(databaseName,
        s"""|SELECT
            |   symbol AS ticker,
            |   exchange AS market,
            |   lastSale,
            |   ROUND(lastSale, 1) AS roundedLastSale,
            |   lastTradeTime AS lastSaleTime
            |FROM $tableName
            |ORDER BY lastSale DESC
            |LIMIT 5
            |""".stripMargin
      )

      val outcome = solution.get
      assert(outcome.isLeft)
      val results = outcome.left.get
      assert(results.columns.map(_.name).toSet == Set("ticker", "market", "lastSale", "roundedLastSale", "lastSaleTime"))
      assert(results.length == 5)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
    }

    it("should perform aggregate queries") {
      val solution = cpu.executeQuery(databaseName,
        s"""|SELECT
            |   exchange AS market,
            |   COUNT(*) AS total,
            |   COUNT(DISTINCT(symbol)) AS tickers,
            |   AVG(lastSale) AS avgLastSale,
            |   MIN(lastSale) AS minLastSale,
            |   MAX(lastSale) AS maxLastSale,
            |   SUM(lastSale) AS sumLastSale
            |FROM $tableName
            |GROUP BY exchange
            |ORDER BY market DESC
            |""".stripMargin
      )

      val outcome = solution.get
      assert(outcome.isLeft)
      val results = outcome.left.get
      assert(results.columns.map(_.name).toSet == Set("market", "total", "tickers", "avgLastSale", "sumLastSale", "maxLastSale", "minLastSale"))
      assert(results.length == 4)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
    }

    it("should DROP a VIEW") {
      cpu.executeQuery(databaseName, sql = s"DROP VIEW IF EXISTS $viewName")
      assert(!DatabaseFiles.isVirtualTable(databaseName, viewName))
    }

    it("should CREATE a VIEW") {
      cpu.executeQuery(databaseName, sql =
        s"""|CREATE VIEW IF NOT EXISTS $viewName
            |WITH DESCRIPTION 'NYSE Stock symbols sorted by last sale'
            |AS
            |SELECT
            |   symbol AS ticker,
            |   exchange AS market,
            |   lastSale,
            |   ROUND(lastSale, 1) AS roundedLastSale,
            |   lastTradeTime AS lastSaleTime
            |FROM $tableName
            |WHERE exchange = 'NYSE'
            |ORDER BY lastSale DESC
            |LIMIT 50
            |""".stripMargin
      )
      assert(DatabaseFiles.isVirtualTable(databaseName, viewName))
    }

    it("should query rows from the VIEW") {
      val solution = cpu.executeQuery(databaseName, sql = s"SELECT * FROM $viewName LIMIT 5")
      val outcome = solution.get
      assert(outcome.isLeft)
      val results = outcome.left.get
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
      assert(results.length == 5)
      assert(results.columns.map(_.name).toSet == Set("market", "roundedLastSale", "lastSale", "lastSaleTime", "ticker"))
    }

  }

}
