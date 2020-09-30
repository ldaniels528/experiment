package com.qwery.database
package server

import java.io.File
import java.nio.ByteBuffer
import java.util.Date

import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val service: ServerSideTableService = new ServerSideTableService()

  describe(classOf[TableFile].getName) {
    val databaseName = DEFAULT_DATABASE
    val tableName = "stocks"

    it("should create a new table and insert new rows into it") {
      TableFile.dropTable(databaseName, tableName)
      TableFile.createTable(databaseName, tableName,
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastTradeTime", comment = "the latest sale date/time", ColumnMetadata(`type` = ColumnTypes.DateType))
        )) use { table =>
        service.executeQuery(databaseName, "TRUNCATE stocks")
        logger.info(s"${table.tableName}: truncated - ${table.count()} records")

        table.insert(Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastTradeTime" -> new Date()))

        service.executeQuery(databaseName,
          """|INSERT INTO stocks (symbol, exchange, lastSale, lastTradeTime)
             |VALUES ('MSFT', 'NYSE', 167.55, 1601064578145)
             |""".stripMargin
        )

        val count = table.count()
        logger.info(s"$databaseName.${table.tableName}: inserted $count records")
        assert(count == 7)
      }
    }

    it("should create an index on a table") {
      service.executeQuery(databaseName, "CREATE INDEX stocks_symbol ON stocks (symbol)")
    }

    it("should read a row from a table") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        val row = table.get(0)
        logger.info(s"[0] $row")
      }
    }

    it("should count the number of rows in a table") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        val count = table.count(condition = Map("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        val results = table.findRows(limit = None, condition = Map("exchange" -> "NASDAQ"))
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should query rows via a condition from a table") {
       TableFile(databaseName, tableName = "stocks") use { table =>
         val results = service.executeQuery(databaseName, "SELECT * FROM stocks WHERE exchange = 'NASDAQ'")
         results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
       }
    }

    it("should update rows in a table") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        val count = table.update(values = Map("lastSale" -> 0.50), condition = Map("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should delete a row from a table") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        table.delete(0)
        val results = service.executeQuery(databaseName, "SELECT * FROM stocks")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should handle mass inserts into a table from a file") {
      TableFile(databaseName, tableName = "stocks") use { table =>
        val count = table.load(new File("./stocks.csv"))(_.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            Map("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> date.toLong)
          case _ => Map.empty
        })
        logger.info(s"Loaded $count items")

        val results = service.executeQuery(databaseName, "SELECT * FROM stocks LIMIT 20")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should build an index and find a row using it") {
      val symbol = "MSFT"
      TableFile(databaseName, tableName = "stocks") use { table =>
        // create the index
        val tableDevice = table.device
        val tableColumns = tableDevice.columns
        val indexDevice = table.createIndex(
          indexName = "stocks_symbol",
          indexColumn = tableColumns(tableColumns.indexWhere(_.name == "symbol"))
        )

        // display the results
        logger.info(s"index.length is ${indexDevice.length}, recordSize is ${indexDevice.recordSize}")
          indexDevice foreach { case (rowID, buf) =>
            showBuffer(rowID, buf)(indexDevice)
          }

        // find a value
        logger.info(s"Looking for '$symbol'...")
        val (rowID_?, processedTime) = time(table.binarySearch(indexDevice, "symbol", Some(symbol)))
        logger.info(s"rowID => ${rowID_?}")
        logger.info(f"Query took $processedTime%.2f msec")
        assert(rowID_?.nonEmpty)

        // retrieve the row
        rowID_? foreach { rowID =>
          val row = table.get(rowID)
          assert(row.nonEmpty)
          logger.info(f"[$rowID%04d] $row")
        }
      }
    }

  }

  private def showBuffer(rowID: ROWID, buf: ByteBuffer)(implicit indexDevice: BlockDevice): Unit = {
    val row = Map(indexDevice.columns.zipWithIndex flatMap { case (column, idx) =>
      buf.position(indexDevice.columnOffsets(idx))
      val (_, value_?) = Codec.decode(buf)
      value_?.map(value => column.name -> value)
    }: _*)
    logger.info(f"$rowID%d - $row")
  }

}
