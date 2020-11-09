package com.qwery.database

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

    it(s"should insert $insertCount rows into a table") {
      val queryProcessor = new QueryProcessor(requestTimeout = 10.seconds)
      import queryProcessor.dispatcher

      val databaseName = "test"
      val tableName = "stocks_test"
      val newQuote = () => RowTuple("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastTradeTime" -> randomDate)

      val startTime = System.nanoTime()
      val outcomes = for {
        _ <- queryProcessor.truncateTable(databaseName, tableName)
        responses <- Future.sequence((1 to insertCount).map(_ => queryProcessor.insertRow(databaseName, tableName, newQuote())))
        result <- queryProcessor.getRow(databaseName, tableName, insertCount / 2)
      } yield (result, responses)

      val (result, responses) = Await.result(outcomes, Duration.Inf)
      val elapsedTime = (System.nanoTime() - startTime) / 1e+6
      logger.info(f"Inserted ${responses.length} records in $elapsedTime%.1f msec")
      logger.info(f"Retrieved row $result")
      responses.take(20).zipWithIndex foreach { case (response, n) => logger.info(f"insert[$n%03d]: $response") }
    }

  }

}
