package com.qwery.database.device

import com.qwery.database.files.{LoadMetrics, TableFile, TableProperties}
import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, KeyValues, time}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Date

/**
 * Table Index Device Test Suite
 * @author lawrence.daniels@gmail.com
 */
class TableIndexDeviceTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  val databaseName = "test"
  val tableName = "stocks_idx_test"
  val indexColumn = "symbol"

  describe(classOf[TableIndexDevice].getName) {

    it("should create and index a table") {
      // drop the previous table (if it exists)
      TableFile.dropTable(databaseName, tableName, ifExists = true)

      // create the table
      TableFile.createTable(databaseName, tableName, properties = TableProperties.create(
        description = Some("index test table"),
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastTradeTime", comment = "the latest sale date/time", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DateType))
        )))

      TableFile(databaseName, tableName).use { table =>
        // create the table index
        val indexDevice = table.createIndex(indexColumn)

        // create some records
        table.insertRow(makeRow(symbol = "AAPL", exchange = "NASDAQ", lastSale = 275.88, lastTradeTime = new Date()))
        table.insertRow(makeRow(symbol = "MSFT", exchange = "NASDAQ", lastSale = 78.45, lastTradeTime = new Date()))
        table.insertRow(makeRow(symbol = "GE", exchange = "NASDAQ", lastSale = 43.55, lastTradeTime = new Date()))
        table.insertRow(makeRow(symbol = "AMD", exchange = "NASDAQ", lastSale = 67.11, lastTradeTime = new Date()))
        table.insertRow(makeRow(symbol = "IBM", exchange = "NASDAQ", lastSale = 275.88, lastTradeTime = new Date()))
        table.insertRow(makeRow(symbol = "TWTR", exchange = "NASDAQ", lastSale = 98.17, lastTradeTime = new Date()))

        // show the contents of the index
        indexDevice.rebuild()(table.device)
        dump(indexDevice)
        assert(indexDevice.length == 6)

        // search for the row containing "AAPL" (e.g. find value via the index)
        val (rows, processedTime) = time(table.getRows(condition = KeyValues("symbol" -> "AAPL")))
        logger.info(f"Retrieved ${rows.length} rows via index in $processedTime%.2f msec")
        rows.foreach(row => logger.info(row.toKeyValues.toString))
        assert(rows.length == 1)
      }
    }

    it("should update the index when updating a record") {
      TableFile(databaseName, tableName).use { table =>
        // update some rows
        val (count, updateTime) = time(table.updateRows(values = KeyValues("symbol" -> "AMDX"), condition = KeyValues("symbol" -> "AMD")))
        logger.info(f"Updated $count rows via index in $updateTime%.2f msec")

        val indexDevice = TableIndexDevice(TableIndexRef(databaseName, tableName, indexColumn))
        dump(indexDevice)
      }
    }

    it("should update the index when deleting a record") {
      TableFile(databaseName, tableName).use { table =>
        // update some rows
        val (count, updateTime) = time(table.deleteRows(condition = KeyValues("symbol" -> "AMDX")))
        logger.info(f"Deleted $count rows via index in $updateTime%.2f msec")

        val indexDevice = TableIndexDevice(TableIndexRef(databaseName, tableName, indexColumn))
        dump(indexDevice)
      }
    }

  }

  def copyInto(databaseName: String, tableName: String, file: File): LoadMetrics = {
    TableFile(databaseName, tableName) use { table =>
      val metrics = table.ingestTextFile(file)(_.split("[,]") match {
        case Array(symbol, exchange, price, date) =>
          Option(KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> new Date(date.toLong)))
        case _ => None
      })
      logger.info(s"metrics: $metrics")
      metrics
    }
  }

  def dump(device: BlockDevice): Unit = {
    logger.info(s"show the contents of the index: [length = ${device.length}]")
    device foreach { row =>
      logger.info(f"[${row.id}%04d] ${row.toKeyValues}")
    }
    logger.info("")
  }

  def makeRow(symbol: String, exchange: String, lastSale: Double, lastTradeTime: Date): KeyValues = {
    KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> lastSale, "lastTradeTime" -> lastTradeTime)
  }

}
