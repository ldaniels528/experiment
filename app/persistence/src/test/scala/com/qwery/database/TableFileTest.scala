package com.qwery.database

import java.io.File

import com.qwery.database.server.{TableFile, TableManager}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val tableManager: TableManager = new TableManager()

  describe(classOf[TableFile].getName) {

    it("should create a new table and insert new rows into it") {
      TableFile.create(
        name = "stocks",
        columns = Seq(
          Column(name = "symbol", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", metadata = ColumnMetadata(`type` = ColumnTypes.DoubleType), maxSize = None),
          Column(name = "lastSaleTime", metadata = ColumnMetadata(`type` = ColumnTypes.LongType), maxSize = None)
        )) use { table =>
        tableManager.query("TRUNCATE stocks")
        logger.info(s"${table.name}: truncated - ${table.count()} records")

        table.insert(Map('symbol -> "AMD", 'exchange -> "NASDAQ", 'lastSale -> 67.55, 'lastSaleTime -> System.currentTimeMillis()))
        table.insert(Map('symbol -> "AAPL", 'exchange -> "NYSE", 'lastSale -> 123.55, 'lastSaleTime -> System.currentTimeMillis()))
        table.insert(Map('symbol -> "GE", 'exchange -> "NASDAQ", 'lastSale -> 89.55, 'lastSaleTime -> System.currentTimeMillis()))
        table.insert(Map('symbol -> "PEREZ", 'exchange -> "OTCBB", 'lastSale -> 0.001, 'lastSaleTime -> System.currentTimeMillis()))
        table.insert(Map('symbol -> "AMZN", 'exchange -> "NYSE", 'lastSale -> 1234.55, 'lastSaleTime -> System.currentTimeMillis()))
        table.insert(Map('symbol -> "INTC", 'exchange -> "NYSE", 'lastSale -> 56.55, 'lastSaleTime -> System.currentTimeMillis()))
        val count = table.count()
        logger.info(s"${table.name}: inserted $count records")
        assert(count == 6)
      }
    }

    it("should read a row from a table") {
      TableFile(name = "stocks") use { table =>
        val row = table.get(0)
        logger.info(s"[0] $row")
      }
    }

    it("should count the number of rows in a table") {
      TableFile(name = "stocks") use { table =>
        val count = table.count(condition = Map('exchange -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(name = "stocks") use { table =>
        val results = table.find(limit = None, condition = Map('exchange -> "NASDAQ"))
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should query rows via a condition from a table") {
       TableFile(name = "stocks") use { table =>
         val results = tableManager.query("SELECT * FROM stocks WHERE exchange = 'NASDAQ'")
         results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
       }
    }

    it("should update rows in a table") {
      TableFile(name = "stocks") use { table =>
        val count = table.update(values = Map('lastSale -> 0.50), condition = Map('symbol -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should delete a row from a table") {
      TableFile(name = "stocks") use { table =>
        table.delete(0)
        val results = tableManager.query("SELECT * FROM stocks")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should handle mass inserts into a table") {
      TableFile(name = "stocks") use { table =>
        val count = table.load(new File("./stocks.csv"))(_.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            Map('symbol -> symbol, 'exchange -> exchange, 'lastSale -> price.toDouble, 'lastSaleTime -> date.toLong)
          case _ => Map.empty
        })
        logger.info(s"Loaded $count items")

        val results = tableManager.query("SELECT * FROM stocks LIMIT 20")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

  }

}
