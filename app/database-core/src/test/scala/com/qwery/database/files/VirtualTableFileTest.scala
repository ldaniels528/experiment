package com.qwery.database
package files

import com.qwery.database.models.StockQuote.{randomDate, randomExchange, randomPrice, randomSymbol}
import com.qwery.database.models.{Column, ColumnMetadata, ColumnTypes, KeyValues, TableProperties}
import com.qwery.language.SQLLanguageParser
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Virtual Table File Test
  */
class VirtualTableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  val databaseName = "test"
  val tableName = "tickers"
  val viewName = "tickers_NYSE"

  val newQuote: () => KeyValues = {
    () => KeyValues("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastSaleTime" -> randomDate)
  }

  describe(classOf[ExternalTableFile].getName) {

    it("should prepare a sample data table") {
      TableFile.dropTable(databaseName, tableName, ifExists = true)
      TableFile.createTable(databaseName, tableName, TableProperties.create(
        description = Some("table to test inserting records"),
        columns = Seq(
          Column.create(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column.create(name = "exchange", comment = "the stock exchange", enumValues = Nil, models.ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column.create(name = "lastSale", comment = "the latest sale price", enumValues = Nil, models.ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column.create(name = "lastSaleTime", comment = "the latest sale date/time", enumValues = Nil, models.ColumnMetadata(`type` = ColumnTypes.DateType))
        ))) use { table =>
        table.truncate()
        logger.info(s"${table.tableName}: truncated - ${table.count()} records")

        val n_tickers = 1e+5.toInt
        for(_ <- 1 to n_tickers) table.insertRow(newQuote())
        val count = table.count()
        logger.info(s"$databaseName.$tableName: inserted $count records")
        assert(count == n_tickers)
      }
    }

    it("should perform DROP VIEW IF EXISTS") {
      VirtualTableFile.dropView(databaseName, viewName, ifExists = true)
    }

    it("should perform CREATE VIEW") {
      VirtualTableFile.createView(databaseName, viewName,
        description = Some("AMEX Stock symbols sorted by last sale"),
        ifNotExists = false,
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
    }


  }

}