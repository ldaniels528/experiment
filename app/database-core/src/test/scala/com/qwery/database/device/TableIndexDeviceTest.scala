package com.qwery.database.device

import com.qwery.database.files.TableFile
import com.qwery.database.models.{KeyValues, LoadMetrics}
import com.qwery.database.time
import com.qwery.models.{ColumnSpec, EntityRef, Table, TableIndex, Column => XColumn}
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
  val schemaName = "stocks"
  val indexColumn = "symbol"
  val tableRef = new EntityRef(databaseName, schemaName, tableName)

  describe(classOf[TableIndexDevice].getName) {

    it("should create and index a table") {
      // drop the previous table (if it exists)
      TableFile.dropTable(tableRef, ifExists = true)

      // create the table
      TableFile.createTable(table = Table(
        ref = tableRef,
        description = Some("index test table"),
        columns = List(
          XColumn(name = "symbol", comment = Some("the ticker symbol"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "exchange", comment = Some("the stock exchange"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "lastSale", comment = Some("the latest sale price"), spec = ColumnSpec(typeName = "Double")),
          XColumn(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = ColumnSpec(typeName = "DateTime"))
        )))

      TableFile(tableRef).use { table =>
        // create the table index
        val indexDevice = table.createIndex(TableIndex(tableRef, tableRef, columns = Seq(indexColumn), ifNotExists = false))

        // create some records
        table.insertRow(makeRow(symbol = "AAPL", exchange = "NASDAQ", lastSale = 275.88, lastSaleTime = new Date()))
        table.insertRow(makeRow(symbol = "MSFT", exchange = "NASDAQ", lastSale = 78.45, lastSaleTime = new Date()))
        table.insertRow(makeRow(symbol = "GE", exchange = "NASDAQ", lastSale = 43.55, lastSaleTime = new Date()))
        table.insertRow(makeRow(symbol = "AMD", exchange = "NASDAQ", lastSale = 67.11, lastSaleTime = new Date()))
        table.insertRow(makeRow(symbol = "IBM", exchange = "NASDAQ", lastSale = 275.88, lastSaleTime = new Date()))
        table.insertRow(makeRow(symbol = "TWTR", exchange = "NASDAQ", lastSale = 98.17, lastSaleTime = new Date()))

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
      TableFile(tableRef).use { table =>
        // update some rows
        val (count, updateTime) = time(table.updateRows(values = KeyValues("symbol" -> "AMDX"), condition = KeyValues("symbol" -> "AMD")))
        logger.info(f"Updated $count rows via index in $updateTime%.2f msec")

        val indexDevice = TableIndexDevice(TableIndex(tableRef, tableRef, columns = Seq(indexColumn), ifNotExists = false))
        dump(indexDevice)
      }
    }

    it("should update the index when deleting a record") {
      TableFile(tableRef).use { table =>
        // update some rows
        val (count, updateTime) = time(table.deleteRows(condition = KeyValues("symbol" -> "AMDX")))
        logger.info(f"Deleted $count rows via index in $updateTime%.2f msec")

        val indexDevice = TableIndexDevice(TableIndex(tableRef, tableRef, columns = Seq(indexColumn), ifNotExists = false))
        dump(indexDevice)
      }
    }

  }

  def copyInto(tableRef: EntityRef, file: File): LoadMetrics = {
    TableFile(tableRef) use { table =>
      val metrics = table.ingestTextFile(file)(_.split("[,]") match {
        case Array(symbol, exchange, price, date) =>
          Option(KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastSaleTime" -> new Date(date.toLong)))
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

  def makeRow(symbol: String, exchange: String, lastSale: Double, lastSaleTime: Date): KeyValues = {
    KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> lastSale, "lastSaleTime" -> lastSaleTime)
  }

}
