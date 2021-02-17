package com.qwery.database
package server

import akka.util.Timeout
import com.qwery.database.server.StockQuote._
import com.qwery.language.SQLLanguageParser
import com.qwery.models.expressions.AllFields
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.Date
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Query Processor Test Suite
 */
class QueryProcessorTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val insertCount = 1e+5.toInt
  private implicit val timeout: Timeout = 2.minutes

  describe(classOf[QueryProcessor].getName) {
    val queryProcessor = new QueryProcessor()
    import queryProcessor.dispatcher

    val databaseName = "test"
    val tableName = "stocks_qp_test"
    val newQuote = () => KeyValues("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastTradeTime" -> randomDate)

    it("should drop the existing table") {
      Await.result(queryProcessor.dropTable(databaseName, tableName, ifExists = true), Duration.Inf)
    }

    it("should create a new table") {
      Await.result(queryProcessor.executeQuery(databaseName, sql =
        s"""|CREATE TABLE IF NOT EXISTS $tableName (
            |   symbol VARCHAR(8) COMMENT "the ticker symbol",
            |   exchange VARCHAR(8) COMMENT "the stock exchange",
            |   lastSale DOUBLE COMMENT "the latest sale price",
            |   lastTradeTime DATETIME COMMENT "the latest sale date/time"
            |) WITH COMMENT 'SQL created table'
            |""".stripMargin), Duration.Inf)
    }

    it("should insert rows into the table") {
      val clock = stopWatch
      val outcomes = for {
        _ <- queryProcessor.truncateTable(databaseName, tableName)
        responses <- Future.sequence((1 to insertCount).map(_ => queryProcessor.insertRow(databaseName, tableName, newQuote())))
        result <- queryProcessor.getRow(databaseName, tableName, insertCount / 2)
      } yield (result, responses)

      val (result, responses) = Await.result(outcomes, Duration.Inf)
      val elapsedTime = clock()
      logger.info(f"Inserted ${responses.length} records in $elapsedTime%.1f msec")
      logger.info(f"Retrieved row $result")
      responses.take(20).zipWithIndex foreach { case (response, n) => logger.info(f"insert[$n%03d]: $response") }
    }

    it("should lock, replace then unlock a row") {
      val rowID: ROWID = 0
      val clock = stopWatch
      val myLockID = Await.result(for {
        // lock the row
        lockID <- queryProcessor.lockRow(databaseName, tableName, rowID)
        _ = logger.info(f"lockID is '$lockID' after ${clock()}%.1f msec")

        // replace the row
        u0 <- queryProcessor.replaceRow(databaseName, tableName, rowID, newQuote())
        _ = logger.info(f"Replaced ${u0.count} records after ${clock()}%.1f msec")

        // unlock the row
        //u1 <- queryProcessor.unlockRow(databaseName, tableName, rowID, lockID)
        //_ = logger.info(f"Updated ${u1.count} records after ${clock()}%.1f msec")

      } yield lockID, Duration.Inf)

      val elapsedTime = clock()
      logger.info(f"Lock/replace/unlock => '$myLockID' in $elapsedTime%.1f msec")
    }

    it("should perform transformation queries") {
      val outcome = queryProcessor.executeQuery(databaseName,
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

      val results = Await.result(outcome, Duration.Inf)
      assert(results.columns.map(_.name).toSet == Set("ticker", "market", "lastSale", "roundedLastSale", "lastSaleTime"))
      assert(results.rows.size == 5)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
    }

    it("should perform aggregate queries") {
      val outcome = queryProcessor.executeQuery(databaseName,
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

      val results = Await.result(outcome, Duration.Inf)
      assert(results.columns.map(_.name).toSet == Set("market", "total", "tickers", "avgLastSale", "sumLastSale", "maxLastSale", "minLastSale"))
      assert(results.rows.size == 4)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
    }

    it("should perform CREATE VIEW (API)") {
      val viewName = "tickers_AMEX"
      val outcome = for {
        _ <- queryProcessor.dropView(databaseName, viewName, ifExists = true)
        _ <- queryProcessor.createView(databaseName, viewName, ifNotExists = true,
          description = Some("AMEX Stock symbols sorted by last sale"),
          invokable = SQLLanguageParser.parse(
            s"""|SELECT
                |   symbol AS ticker,
                |   exchange AS market,
                |   lastSale,
                |   ROUND(lastSale, 1) AS roundedLastSale,
                |   lastTradeTime AS lastSaleTime
                |FROM $tableName
                |WHERE exchange = 'AMEX'
                |ORDER BY lastSale DESC
                |LIMIT 50
              |""".stripMargin
        ))
        results <- queryProcessor.selectRows(databaseName, viewName, fields = Seq(AllFields), limit = Some(5))
      } yield results

      val results = Await.result(outcome, Duration.Inf)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
      assert(results.rows.size == 5)
      assert(results.columns.map(_.name).toSet == Set("market", "roundedLastSale", "lastSale", "lastSaleTime", "ticker"))
    }

    it("should perform CREATE VIEW (SQL)") {
      val viewName = "tickers_NYSE"
      val outcome = for {
        _ <- queryProcessor.executeQuery(databaseName, sql = s"DROP VIEW IF EXISTS $viewName")
        _ <- queryProcessor.executeQuery(databaseName, sql =
          s"""|CREATE VIEW $viewName
              |WITH COMMENT 'NYSE Stock symbols sorted by last sale'
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
        results <- queryProcessor.executeQuery(databaseName, sql = s"SELECT * FROM $viewName LIMIT 5")
      } yield results

      val results = Await.result(outcome, Duration.Inf)
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
      assert(results.rows.size == 5)
      assert(results.columns.map(_.name).toSet == Set("market", "roundedLastSale", "lastSale", "lastSaleTime", "ticker"))
    }

    it("should list desired columns from all tables within a database") {
      val list = Await.result(queryProcessor.searchColumns(databaseNamePattern = Some(databaseName), tableNamePattern = None, columnNamePattern = Some("symbol")), Duration.Inf)
      logger.info(f"${"databaseName"}%-25s ${"tableName"}%-25s ${"columnName"}%-25s")
      list foreach { tableInfo =>
        logger.info(f"${tableInfo.databaseName}%-25s ${tableInfo.tableName}%-25s ${tableInfo.column.name}%-25s")
      }
    }

  }

  def makeRow(symbol: String, exchange: String, lastSale: Double, lastTradeTime: Date): KeyValues = {
    KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> lastSale, "lastTradeTime" -> lastTradeTime)
  }

}
