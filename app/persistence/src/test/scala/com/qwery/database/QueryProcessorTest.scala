package com.qwery.database

import java.util.Date

import com.qwery.database.StockQuote._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, Future}

/**
 * Query Processor Test Suite
 */
class QueryProcessorTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val insertCount = 1e+5.toInt

  describe(classOf[QueryProcessor].getName) {
    val queryProcessor = new QueryProcessor(requestTimeout = 10.seconds)
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
            |)
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
            |LIMIT 5
            |""".stripMargin
      )

      val results = Await.result(outcome, Duration.Inf)
      assert(results.columns.map(_.name).toSet == Set("ticker", "market", "lastSale", "roundedLastSale", "lastSaleTime"))
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
            |""".stripMargin
      )

      val results = Await.result(outcome, Duration.Inf)
      assert(results.columns.map(_.name).toSet == Set("market", "total", "tickers", "avgLastSale", "sumLastSale", "maxLastSale", "minLastSale"))
      results foreachKVP { row =>
        logger.info(s"row: $row")
      }
    }

  }

  def makeRow(symbol: String, exchange: String, lastSale: Double, lastTradeTime: Date): KeyValues = {
    KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> lastSale, "lastTradeTime" -> lastTradeTime)
  }

}
