package com.qwery.database
package files

import com.qwery.database.models.KeyValues
import com.qwery.database.models.StockQuote.{randomDate, randomExchange, randomPrice, randomSymbol}
import com.qwery.language.SQLLanguageParser
import com.qwery.models.{ColumnTypeSpec, EntityRef, Table, View, Column}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
  * Virtual Table File Test
  */
class VirtualTableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  val databaseName = "test"
  val schemaName = "stocks"
  val tableName = "tickers"
  val viewName = "tickers_NYSE"

  val tableRef = new EntityRef(databaseName, schemaName, tableName)
  val viewRef = new EntityRef(databaseName, schemaName, viewName)

  val newQuote: () => KeyValues = {
    () => KeyValues("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastSaleTime" -> randomDate)
  }

  describe(classOf[ExternalTableFile].getName) {

    it("should prepare a sample data table") {
      TableFile.dropTable(tableRef, ifExists = true)
      TableFile.createTable(Table(
        ref = tableRef,
        description = Some("table to test inserting records"),
        columns = List(
          Column(name = "symbol", comment = Some("the ticker symbol"), spec = new ColumnTypeSpec(`type` = "String", size = 8)),
          Column(name = "exchange", comment = Some("the stock exchange"), spec = new ColumnTypeSpec(`type` = "String", size = 8)),
          Column(name = "lastSale", comment = Some("the latest sale price"), spec = new ColumnTypeSpec(`type` = "Double")),
          Column(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = new ColumnTypeSpec(`type` = "DateTime"))
        ))) use { table =>
        table.truncate()
        logger.info(s"${tableRef.name}: truncated - ${table.count()} records")

        val n_tickers = 1e+5.toInt
        for(_ <- 1 to n_tickers) table.insertRow(newQuote())
        val count = table.count()
        logger.info(s"$databaseName.$schemaName.$tableName: inserted $count records")
        assert(count == n_tickers)
      }
    }

    it("should perform DROP VIEW IF EXISTS") {
      VirtualTableFile.dropView(viewRef, ifExists = true)
    }

    it("should perform CREATE VIEW") {
      VirtualTableFile.createView(View(viewRef,
        description = Some("AMEX Stock symbols sorted by last sale"),
        query = SQLLanguageParser.parse(
          s"""|SELECT
              |   symbol AS ticker,
              |   exchange AS market,
              |   lastSale,
              |   ROUND(lastSale, 1) AS roundedLastSale,
              |   lastSaleTime
              |FROM `$databaseName.$schemaName.$tableName`
              |WHERE exchange = 'AMEX'
              |ORDER BY lastSale DESC
              |LIMIT 50
              |""".stripMargin
        )))
    }

  }

}