package com.qwery.database

import java.io.File
import java.nio.ByteBuffer
import java.util.Date

import com.qwery.database.device.BlockDevice
import com.qwery.database.models.{LoadMetrics, TableIndexRef}
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
      TableFile.dropTable(databaseName, tableName)
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
        val row = table.get(0)
        logger.info(s"[0] $row")
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
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should query rows via a condition from a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.selectRows(fields = List(AllFields), where = RowTuple("exchange" -> "NASDAQ"))
        for {
          row <- results.rows
          (result, index) <- row.zipWithIndex
        } logger.info(s"[$index] $result")
      }
    }

    it("should update rows in a table") {
      TableFile(databaseName, tableName) use { table =>
        val count = table.updateRows(values = RowTuple("lastSale" -> 0.50), condition = RowTuple("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should delete a row from a table") {
      TableFile(databaseName, tableName) use { table =>
        table.deleteRow(0)
        val results = table.selectRows(fields = List(AllFields), where = RowTuple())
        for {
          row <- results.rows
          (result, index) <- row.zipWithIndex
        } logger.info(s"[$index] $result")
      }
    }

    it("should build an index and find a row using it (Scala)") {
      val tableName = "stocks_def"
      val indexColumn = "symbol"
      val indexName = s"${tableName}_$indexColumn"
      val searchSymbol = "MSFT"

      // drop the previous table (if it exists)
      TableFile.dropTable(databaseName, tableName)

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
        // insert the MSFT record
        table.insertRow(RowTuple("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 98.55, "lastTradeTime" -> new Date()))

        // create the table index
        val (indexDevice, indexCreationTime) = time(table.createIndex(indexName, indexColumn))
        logger.info(f"Created index '$indexName' in $indexCreationTime%.2f msec")

        // display the index rows (debug-only)
        indexDevice foreachBuffer { case (rowID, buf) => showBuffer(rowID, buf)(indexDevice) }

        // search for a row (e.g. find value via the index)
        val (row_?, processedTime) = time(for {
          indexRow <- table.binarySearch(TableIndexRef(indexName, indexColumn), searchValue = Option(searchSymbol))
          dataRowID <- indexRow.fields.find(_.name == "rowID").flatMap(_.value.map(_.asInstanceOf[ROWID]))
          dataRow <- table.get(dataRowID)
        } yield dataRow)
        logger.info(f"Retrieved row #${row_?.map(_.rowID).getOrElse("[none]")} via index '$indexName' in $processedTime%.2f msec")

        assert(row_?.nonEmpty)
        row_?.foreach(row => logger.info(f"row: $row"))
      }
    }

  }

  def copyInto(databaseName: String, tableName: String, file: File): LoadMetrics = {
    TableFile(databaseName, tableName) use { table =>
      val metrics = table.load(file)(_.split("[,]") match {
        case Array(symbol, exchange, price, date) =>
          RowTuple("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> new Date(date.toLong))
        case _ => RowTuple()
      })
      logger.info(s"metrics: $metrics")
      metrics
    }
  }

  def showBuffer(rowID: ROWID, buf: ByteBuffer)(implicit indexDevice: BlockDevice): Unit = {
    val row = RowTuple(indexDevice.columns.zipWithIndex flatMap { case (column, idx) =>
      buf.position(indexDevice.columnOffsets(idx))
      val (_, value_?) = Codec.decode(column, buf)
      value_?.map(value => column.name -> value)
    }: _*)
    logger.debug(f"$rowID%d - $row")
  }

}
