package com.qwery.database
package server

import java.io.File
import java.nio.ByteBuffer

import com.qwery.util.ResourceHelper._
import com.qwery.util.ServicingTools.time
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val tables: ServerSideTableService = new ServerSideTableService()

  describe(classOf[TableFile].getName) {

    it("should create a new table and insert new rows into it") {
      TableFile.create(
        name = "stocks",
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", metadata = ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastSaleTime", comment = "the latest sale date/time", metadata = ColumnMetadata(`type` = ColumnTypes.LongType))
        )) use { table =>
        tables.executeQuery("TRUNCATE stocks")
        logger.info(s"${table.tableName}: truncated - ${table.count()} records")

        table.insert(Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastSaleTime" -> System.currentTimeMillis()))
        table.insert(Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis()))
        table.insert(Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastSaleTime" -> System.currentTimeMillis()))
        table.insert(Map("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastSaleTime" -> System.currentTimeMillis()))
        table.insert(Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastSaleTime" -> System.currentTimeMillis()))
        table.insert(Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastSaleTime" -> System.currentTimeMillis()))

        tables.executeQuery(
          """|INSERT INTO stocks (symbol, exchange, lastSale, lastSaleTime)
             |VALUES ('MSFT', 'NYSE', 167.55, 1601064578145)
             |""".stripMargin
        )

        val count = table.count()
        logger.info(s"${table.tableName}: inserted $count records")
        assert(count == 7)
      }
    }

    it("should create an index on a table") {
      tables.executeQuery("CREATE INDEX stocks_symbol ON stocks (symbol)")
    }

    it("should read a row from a table") {
      TableFile(name = "stocks") use { table =>
        val row = table.get(0)
        logger.info(s"[0] $row")
      }
    }

    it("should count the number of rows in a table") {
      TableFile(name = "stocks") use { table =>
        val count = table.count(condition = Map("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(name = "stocks") use { table =>
        val results = table.findRows(limit = None, condition = Map("exchange" -> "NASDAQ"))
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should query rows via a condition from a table") {
       TableFile(name = "stocks") use { table =>
         val results = tables.executeQuery("SELECT * FROM stocks WHERE exchange = 'NASDAQ'")
         results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
       }
    }

    it("should update rows in a table") {
      TableFile(name = "stocks") use { table =>
        val count = table.update(values = Map("lastSale" -> 0.50), condition = Map("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should delete a row from a table") {
      TableFile(name = "stocks") use { table =>
        table.delete(0)
        val results = tables.executeQuery("SELECT * FROM stocks")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should handle mass inserts into a table from a file") {
      TableFile(name = "stocks") use { table =>
        val count = table.load(new File("./stocks.csv"))(_.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            Map("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastSaleTime" -> date.toLong)
          case _ => Map.empty
        })
        logger.info(s"Loaded $count items")

        val results = tables.executeQuery("SELECT * FROM stocks LIMIT 20")
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should build an index and find a row using it") {
      val symbol = "MSFT"
      TableFile(name = "stocks") use { table =>
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
