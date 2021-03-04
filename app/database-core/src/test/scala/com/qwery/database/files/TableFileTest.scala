package com.qwery.database
package files

import com.qwery.database.models.{KeyValues, StockQuoteWithDate}
import com.qwery.models.expressions.{AllFields, BasicFieldRef}
import com.qwery.models.{ColumnSpec, Table, EntityRef, Column => XColumn}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.Date

/**
  * Table File Test
  */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  val databaseName = "test"
  val schemaName = "stocks"
  val tableName = "stocks_test"
  val tableRef = new EntityRef(databaseName, schemaName, tableName)

  describe(classOf[TableFile].getName) {

    it("should create a new table and insert new rows into it") {
      TableFile.dropTable(tableRef, ifExists = true)
      TableFile.createTable(Table(
        ref = tableRef,
        description = Some("table to test inserting records"),
        columns = List(
          XColumn(name = "symbol", comment = Some("the ticker symbol"), spec = ColumnSpec(typeName = "STRING", precision = List(8))),
          XColumn(name = "exchange", comment = Some("the stock exchange"), spec = ColumnSpec(typeName = "STRING", precision = List(8))),
          XColumn(name = "lastSale", comment = Some("the latest sale price"), spec = ColumnSpec(typeName = "DOUBLE")),
          XColumn(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = ColumnSpec(typeName = "DATETIME"))
        ))) use { table =>
        table.truncate()
        logger.info(s"${tableRef.name}: truncated - ${table.count()} records")

        table.insertRow(KeyValues("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastSaleTime" -> new Date()))

        val count = table.count()
        logger.info(s"$databaseName.$tableName: inserted $count records")
        assert(count == 6)
      }
    }

    it("should expose the table data as a PersistentSeq") {
      TableFile(tableRef) use { table =>
        val ps = table.toPersistentSeq[StockQuoteWithDate]
        ps.zipWithIndex.foreach { case (quote, n) =>
          logger.info(f"[$n%04d] $quote")
        }
      }
    }

    it("should perform the equivalent of an INSERT-SELECT") {
      val newTableName = "stocks_insert_select"
      val newTableRef = new EntityRef(databaseName, schemaName, newTableName)
      TableFile.dropTable(newTableRef, ifExists = true)
      TableFile.createTable(Table(
        ref = newTableRef,
        description = Some("table to test INSERT-SELECT"),
        columns = List(
          XColumn(name = "symbol", comment = Some("the ticker symbol"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "exchange", comment = Some("the stock exchange"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "lastSale", comment = Some("the latest sale price"), spec = ColumnSpec(typeName = "Double")),
          XColumn(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = ColumnSpec(typeName = "DateTime"))
        ))) use { newTable =>
        newTable.truncate()
        TableFile(tableRef) use { table =>
          newTable.insertRows(table.device)
        }
        val count = newTable.count()
        logger.info(s"Inserted $count rows into $newTableName from $tableName")
        assert(count == 6)
      }
    }

    it("should read a row from a table") {
      TableFile(tableRef) use { table =>
        val rowID = 0
        val row = table.getRow(rowID)
        logger.info(f"[$rowID%04d] $row")
      }
    }

    it("should count the number of rows in a table") {
      TableFile(tableRef) use { table =>
        val count = table.countRows(condition = KeyValues("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(tableRef) use { table =>
        val results = table.getRows(limit = None, condition = KeyValues("exchange" -> "NASDAQ")).toList
        table.device.columns zip results.zipWithIndex foreach { case (column, (result, index)) =>
          logger.info(s"[$index] ${column.name}: $result")
        }
      }
    }

    it("should query rows via a condition from a table") {
      TableFile(tableRef) use { table =>
        val results = table.selectRows(fields = List(AllFields), where = KeyValues("exchange" -> "NASDAQ"))
        results foreachKVP { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should query rows via a condition from a table and create a new table") {
      TableFile(tableRef) use { table =>
        val resultSet = table.getRows(condition = KeyValues("exchange" -> "NASDAQ"))
        resultSet.foreach(row => logger.info(row.toKeyValues.toMap.toString()))
      }
    }

    it("should update rows in a table") {
      TableFile(tableRef) use { table =>
        val count = table.updateRows(values = KeyValues("lastSale" -> 0.50), condition = KeyValues("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should lock/unlock a row in a table") {
      TableFile(tableRef) use { table =>
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
      TableFile(tableRef) use { table =>
        table.deleteRow(0)
        val results = table.selectRows(fields = List(AllFields), where = KeyValues())
        results foreachKVP { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should aggregate rows") {
      TableFile.dropTable(tableRef, ifExists = true)
      TableFile.createTable(Table(
        ref = tableRef,
        description = Some("aggregation test table"),
        columns = List(
          XColumn(name = "symbol", comment = Some("the ticker symbol"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "exchange", comment = Some("the stock exchange"), spec = ColumnSpec(typeName = "String", precision = List(8))),
          XColumn(name = "lastSale", comment = Some("the latest sale price"), spec = ColumnSpec(typeName = "Double")),
          XColumn(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = ColumnSpec(typeName = "DateTime"))
        ))) use { table =>
        table.truncate()
        logger.info(s"${tableRef.name}: truncated - ${table.count()} records")

        table.insertRow(KeyValues("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastSaleTime" -> new Date()))
        table.insertRow(KeyValues("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastSaleTime" -> new Date()))

        val agg = table.selectRows(
          where = KeyValues(),
          groupBy = Seq(BasicFieldRef(name = "exchange")),
          fields = Seq(BasicFieldRef(name = "exchange"))
        )
        agg foreachKVP { kvp =>
          logger.info(kvp.toString)
        }
      }
    }

  }

}
