package com.qwery.database

import com.qwery.database.server.{TableFile, TableManager}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val tableManager: TableManager = new TableManager()

  describe(classOf[TableFile].getName) {

    it("should create a new table") {
      val table = TableFile.create(
        name = "stocks",
        columns = Seq(
          Column(name = "symbol", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", metadata = ColumnMetadata(`type` = ColumnTypes.DoubleType), maxSize = None),
          Column(name = "lastSaleTime", metadata = ColumnMetadata(`type` = ColumnTypes.LongType), maxSize = None)
        ))
      table.truncate()
      logger.info(s"${table.name}: ${table.device.length} records")
      table.close()
    }

    it("should insert new rows into a table") {
      val table = TableFile(name = "stocks")
      table.insert('symbol -> "AMD", 'exchange -> "NASDAQ", 'lastSale -> 67.55, 'lastSaleTime -> System.currentTimeMillis())
      table.insert('symbol -> "AAPL", 'exchange -> "NYSE", 'lastSale -> 123.55, 'lastSaleTime -> System.currentTimeMillis())
      table.insert('symbol -> "GE", 'exchange -> "NASDAQ", 'lastSale -> 89.55, 'lastSaleTime -> System.currentTimeMillis())
      table.insert('symbol -> "AMZN", 'exchange -> "NYSE", 'lastSale -> 1234.55, 'lastSaleTime -> System.currentTimeMillis())
      table.insert('symbol -> "INTC", 'exchange -> "NYSE", 'lastSale -> 56.55, 'lastSaleTime -> System.currentTimeMillis())
      table.close()
    }

    it("should read a row from a table") {
      val table = TableFile(name = "stocks")
      val row = table.get(0)
      table.close()

      logger.info(s"[0] $row")
    }

    it("should search for rows in a table") {
      val table = TableFile(name = "stocks")
      val results = table.search(limit = None, conditions = 'exchange -> "NASDAQ")
      table.close()

      results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
    }

    it("should query rows from a table") {
      val table = TableFile(name = "stocks")
      val results = tableManager.query("SELECT * FROM stocks")
      table.close()

      results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
    }

    it("should delete a row from a table") {
      val table = TableFile(name = "stocks")
      table.delete(0)
      val results = tableManager.query("SELECT * FROM stocks")
      table.close()

      results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
    }

  }

}
