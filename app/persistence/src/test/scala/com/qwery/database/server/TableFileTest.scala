package com.qwery.database.server

import java.util.Date

import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, KeyValues, ROWID}
import com.qwery.models.expressions.{AllFields, BasicField}
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

        table.insertRow(KeyValues("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastTradeTime" -> new Date()))

        val count = table.count()
        logger.info(s"$databaseName.$tableName: inserted $count records")
        assert(count == 6)
      }
    }

    it("should perform the equivalent of an INSERT-SELECT") {
      val newTableName = "stocks_insert_select"
      TableFile.dropTable(databaseName, newTableName, ifExists = true)
      TableFile.createTable(databaseName, newTableName,
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastTradeTime", comment = "the latest sale date/time", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DateType))
        )) use { newTable =>
        newTable.truncate()
        TableFile(databaseName, tableName) use { table =>
          newTable.insertRows(table.device)
        }
        val count = newTable.count()
        logger.info(s"Inserted $count rows into $newTableName from $tableName")
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
        val count = table.countRows(condition = KeyValues("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.getRows(limit = None, condition = KeyValues("exchange" -> "NASDAQ")).toList
       table.device.columns zip results.zipWithIndex foreach { case (column, (result, index)) =>
         logger.info(s"[$index] ${column.name}: $result")
       }
      }
    }

    it("should query rows via a condition from a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.selectRows(fields = List(AllFields), where = KeyValues("exchange" -> "NASDAQ"))
        results foreachKVP { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should query rows via a condition from a table and create a new table") {
      TableFile(databaseName, tableName) use { table =>
        val resultSet = table.getRows(condition = KeyValues("exchange" -> "NASDAQ"))
        resultSet.foreach(row => logger.info(row.toKeyValues.toMap.toString()))
      }
    }

    it("should update rows in a table") {
      TableFile(databaseName, tableName) use { table =>
        val count = table.updateRows(values = KeyValues("lastSale" -> 0.50), condition = KeyValues("symbol" -> "PEREZ"))
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
          rmd.isUnlocked
        })
      }
    }

    it("should delete a row from a table") {
      TableFile(databaseName, tableName) use { table =>
        table.deleteRow(0)
        val results = table.selectRows(fields = List(AllFields), where = KeyValues())
        results foreachKVP { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should aggregate rows") {
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

        table.insertRow(KeyValues("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastTradeTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastTradeTime" -> new Date()))

        val agg = table.selectRows(
          where= KeyValues(),
          groupBy = Seq(BasicField(name = "exchange")),
          fields = Seq(BasicField(name = "exchange"))
        )
        agg foreachKVP { kvp =>
          logger.info(kvp.toString)
        }
      }
    }

  }

}
