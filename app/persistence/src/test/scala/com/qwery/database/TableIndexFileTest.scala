package com.qwery.database

import java.io.File
import java.util.Date

import com.qwery.database.models.LoadMetrics
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.Random

/**
 * Table Index File Test
 */
class TableIndexFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[TableIndexFile].getName) {
    val databaseName = "test"
    val tableName = "stocks_ghi"
    val indexColumn = "symbol"
    val indexName = s"${tableName}_$indexColumn"
    val searchSymbol = "MSFT"

    it("should build an index and find a row using it (Scala)") {
      // drop the previous table (if it exists)
      TableFile.dropTable(databaseName, tableName, ifExists = true)

      // create the table
      TableFile.createTable(databaseName, tableName, columns = Seq(
        Column(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "exchange", comment = "the stock exchange", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "lastSale", comment = "the latest sale price", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DoubleType)),
        Column(name = "lastTradeTime", comment = "the latest sale date/time", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DateType))
      ))

      // populate the table
      val metrics = copyInto(databaseName, tableName, new File("./stocks.csv"))
      assert(metrics.records == 10001)

      // open the table file for read/write
      TableFile(databaseName, tableName) use { table =>
        // update a random row with a MSFT values
        val randomRowID = new Random().nextInt(table.device.length)
        table.replaceRow(randomRowID, RowTuple(indexColumn -> searchSymbol, "exchange" -> "NYSE", "lastSale" -> 98.55, "lastTradeTime" -> new Date()))

        // create the table index
        val (indexDevice, indexCreationTime) = time(table.createIndex(indexName, indexColumn))
        logger.info(f"Created index '$indexName' in $indexCreationTime%.2f msec")

        // search for the row containing "MSFT" (e.g. find value via the index)
        val (row_?, processedTime) = time(table.findRow(condition = RowTuple(indexColumn -> searchSymbol)))
        logger.info(f"Retrieved row #${row_?.map(_.id).getOrElse("[none]")} via index '$indexName' in $processedTime%.2f msec")

        // verify we've found the correct row
        val data_? = row_?.map(_.toRowTuple)
        assert(data_?.flatMap(_.get(indexColumn)).contains(searchSymbol))
        assert(data_?.flatMap(_.get("exchange")).contains("NYSE"))
        assert(data_?.flatMap(_.get("lastSale")).contains(98.55))
        data_?.foreach(row => logger.info(f"row: $row"))
      }
    }

  }

  def copyInto(databaseName: String, tableName: String, file: File): LoadMetrics = {
    TableFile(databaseName, tableName) use { table =>
      val metrics = table.ingestTextFile(file)(_.split("[,]") match {
        case Array(symbol, exchange, price, date) =>
          Option(RowTuple("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> new Date(date.toLong)))
        case _ => None
      })
      logger.info(s"metrics: $metrics")
      metrics
    }
  }

}
