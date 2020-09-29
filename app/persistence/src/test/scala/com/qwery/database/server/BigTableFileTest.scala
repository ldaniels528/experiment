package com.qwery.database.server

import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, StockQuote}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

/**
 * Big Table File Test Suite
 */
class BigTableFileTest extends AnyFunSpec {
  private implicit val tables: ServerSideTableService = new ServerSideTableService()

  describe(classOf[TableFile].getName) {

    it("should handle ingesting large data sets") {
      val expected = 1e+5.toInt
      TableFile.create(
        name = "test_stocks",
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", metadata = ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastSaleTime", comment = "the latest sale date/time", metadata = ColumnMetadata(`type` = ColumnTypes.LongType))
        )) use { table =>
        // ensure the table is empty
        table.truncate()

        // populate the table with random quotes
        (1 to expected) foreach { _ =>
          val q = StockQuote.randomQuote
          table.insert(Map("symbol" -> q.symbol, "exchange" -> q.exchange, "lastSale" -> q.lastSale, "lastSaleTime" -> q.lastSaleTime))
        }

        // ensure the data was inserted
        assert(table.count() == expected)
      }
    }

    it("should handle indexing large data sets") {
      TableFile(name = "test_stocks") use { table =>
        val columns = table.device.columns
        table.createIndex(indexName = "test_stocks_symbol", indexColumn = columns(columns.indexWhere(_.name == "symbol")))
      }
    }

  }

}
