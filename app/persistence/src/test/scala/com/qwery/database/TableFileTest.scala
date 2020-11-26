package com.qwery.database

import java.util.Date

import com.qwery.models.expressions.AllFields
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[TableFile].getName) {
    val databaseName = "test"
    val tableName = "stocks_test"

    it("should create a new table and insert new rows into it") {
      TableFile.dropTable(databaseName, tableName, ifExists = true)
      TableFile.createTable(databaseName, tableName,
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastTradeTime", comment = "the latest sale date/time", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DateType))
        )) use { table =>
        table.truncate()
        logger.info(s"${table.tableName}: truncated - ${table.count()} records")

        table.insertRow(RowTuple("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastTradeTime" -> new Date()))
        table.insertRow(RowTuple("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastTradeTime" -> new Date()))
        table.insertRow(RowTuple("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastTradeTime" -> new Date()))
        table.insertRow(RowTuple("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastTradeTime" -> new Date()))
        table.insertRow(RowTuple("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastTradeTime" -> new Date()))
        table.insertRow(RowTuple("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastTradeTime" -> new Date()))

        val count = table.count()
        logger.info(s"$databaseName.$tableName: inserted $count records")
        assert(count == 6)
      }
    }

    it("should read a row from a table") {
      TableFile(databaseName, tableName) use { table =>
        val rowID = 0
        val row = table.getRow(rowID)
        logger.info(f"[$rowID%04d] $row")
      }
    }

    it("should count the number of rows in a table") {
      TableFile(databaseName, tableName) use { table =>
        val count = table.countRows(condition = RowTuple("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.findRows(limit = None, condition = RowTuple("exchange" -> "NASDAQ"))
       table.device.columns zip results.zipWithIndex foreach { case (column, (result, index)) =>
         logger.info(s"[$index] ${column.name}: $result")
       }
      }
    }

    it("should query rows via a condition from a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.selectRows(fields = List(AllFields), where = RowTuple("exchange" -> "NASDAQ"))
        for {
          row <- results.rows
          (column, (result, index)) <- table.device.columns zip row.zipWithIndex
        } logger.info(s"[$index] ${column.name}: $result")
      }
    }

    it("should update rows in a table") {
      TableFile(databaseName, tableName) use { table =>
        val count = table.updateRows(values = RowTuple("lastSale" -> 0.50), condition = RowTuple("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should lock/unlock a row in a table") {
      TableFile(databaseName, tableName) use { table =>
        val rowID: ROWID = 0

        // lock and verify
        table.lockRow(rowID)
        assert({
          val rmd = table.device.readRowMetaData(rowID)
          logger.info(s"locked: $rmd")
          rmd.isLocked
        })

        // unlock and verify
        table.unlockRow(rowID)
        assert({
          val rmd = table.device.readRowMetaData(rowID)
          logger.info(s"unlocked: $rmd")
          !rmd.isLocked
        })
      }
    }

    it("should delete a row from a table") {
      TableFile(databaseName, tableName) use { table =>
        table.deleteRow(0)
        val results = table.selectRows(fields = List(AllFields), where = RowTuple())
        for {
          row <- results.rows
          (column, (result, index)) <- table.device.columns zip row.zipWithIndex
        } logger.info(s"[$index] ${column.name}: $result")
      }
    }

  }

}
