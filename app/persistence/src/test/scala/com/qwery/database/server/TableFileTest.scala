package com.qwery.database
package server

import java.io.File
import java.nio.ByteBuffer
import java.util.Date

import com.qwery.database.server.TableService.TableIndexRef
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Table File Test
 */
class TableFileTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val service: ServerSideTableService = ServerSideTableService()

  describe(classOf[TableFile].getName) {
    val databaseName = "test"
    val tableName = "stocks_test"

    it("should create a new table and insert new rows into it") {
      TableFile.dropTable(databaseName, tableName)
      TableFile.createTable(databaseName, tableName,
        columns = Seq(
          Column(name = "symbol", comment = "the ticker symbol", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "exchange", comment = "the stock exchange", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
          Column(name = "lastSale", comment = "the latest sale price", ColumnMetadata(`type` = ColumnTypes.DoubleType)),
          Column(name = "lastTradeTime", comment = "the latest sale date/time", ColumnMetadata(`type` = ColumnTypes.DateType))
        )) use { table =>
        service.executeQuery(databaseName, s"TRUNCATE $tableName")
        logger.info(s"${table.tableName}: truncated - ${table.count()} records")

        table.insert(Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55, "lastTradeTime" -> new Date()))
        table.insert(Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55, "lastTradeTime" -> new Date()))

        service.executeQuery(databaseName,
          s"""|INSERT INTO $tableName (symbol, exchange, lastSale, lastTradeTime)
              |VALUES ('MSFT', 'NYSE', 167.55, 1601064578145)
              |""".stripMargin
        )

        val count = table.count()
        logger.info(s"$databaseName.$tableName: inserted $count records")
        assert(count == 7)
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
        val count = table.count(condition = Map("exchange" -> "NYSE"))
        logger.info(s"NYSE => $count")
      }
    }

    it("should find rows via a condition in a table") {
      TableFile(databaseName, tableName) use { table =>
        val results = table.executeQuery(limit = None, condition = Map("exchange" -> "NASDAQ"))
        results.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

    it("should query rows via a condition from a table") {
       TableFile(databaseName, tableName) use { table =>
         val results = service.executeQuery(databaseName, s"SELECT * FROM $tableName WHERE exchange = 'NASDAQ'")
         for {
           row <- results.rows
         } {
           row.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
         }
       }
    }

    it("should update rows in a table") {
      TableFile(databaseName, tableName) use { table =>
        val count = table.update(values = Map("lastSale" -> 0.50), condition = Map("symbol" -> "PEREZ"))
        logger.info(s"update count => $count")
      }
    }

    it("should delete a row from a table") {
      TableFile(databaseName, tableName) use { table =>
        table.delete(0)
        val results = service.executeQuery(databaseName, s"SELECT * FROM $tableName")
        for {
          row <- results.rows
        } {
          row.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
        }
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
        Column(name = "symbol", comment = "the ticker symbol", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "exchange", comment = "the stock exchange", ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "lastSale", comment = "the latest sale price", ColumnMetadata(`type` = ColumnTypes.DoubleType)),
        Column(name = "lastTradeTime", comment = "the latest sale date/time", ColumnMetadata(`type` = ColumnTypes.DateType))
      ))

      // populate the table
      copyInto(databaseName, tableName, new File("./stocks.csv"))

      // open the table file for read/write
      TableFile(databaseName, tableName) use { table =>
        // insert the MSFT record
        table.insert(Map("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 98.55, "lastTradeTime" -> new Date()))

        // create the table index
        val (indexDevice, indexCreationTime) = {
          val tableColumns = table.device.columns
          time(table.createIndex(indexName, tableColumns(tableColumns.indexWhere(_.name == indexColumn))))
        }
        logger.info(f"Created index '$indexName' in $indexCreationTime%.2f msec")

        // display the index rows (debug-only)
        indexDevice foreachBuffer { case (rowID, buf) => showBuffer(rowID, buf)(indexDevice) }

        // search for a row (e.g. find value via the index)
        val (row_?, processedTime) = time(for {
          indexRow <- table.binarySearch(TableIndexRef(indexName, indexColumn), searchValue = Option(searchSymbol))
          rowID <- indexRow.fields.find(_.name == "rowID").flatMap(_.value.map(_.asInstanceOf[ROWID]))
        } yield table.get(rowID))
        logger.info(f"Retrieved row ${row_?} via index '$indexName' in $processedTime%.2f msec")

        assert(row_?.nonEmpty)
        row_?.foreach(row => logger.info(f"row: $row"))
      }
    }

    it("should build an index and find a row using it (SQL)") {
      val tableName = "stocks_abc"
      val indexColumn = "symbol"
      val indexName = s"${tableName}_$indexColumn"
      val searchSymbol = "MSFT"

      // drop the previous table (if it exists)
      service.executeQuery(databaseName, sql = s"DROP TABLE $tableName")

      // create the table
      service.executeQuery(databaseName, sql =
        s"""|CREATE TABLE $tableName (
            |  symbol STRING(8) comment 'the ticker symbol',
            |  exchange STRING(8) comment 'the stock exchange',
            |  lastSale DOUBLE comment 'the latest sale price',
            |  lastTradeTime DATE comment 'the latest sale date/time'
            |)
            |LOCATION '/$databaseName/$tableName/'
            |""".stripMargin
      )

      // populate the table
      copyInto(databaseName, tableName, new File("./stocks.csv"))

      // insert the MSFT record
      service.executeQuery(databaseName, sql =
        s"""|INSERT INTO $tableName (symbol, exchange, lastSale, lastTradeTime)
            |VALUES ("MSFT", "NYSE", 98.55, ${System.currentTimeMillis()})
            |""".stripMargin
      )

      // create the table index
      val (_, indexCreationTime) = time(service.executeQuery(databaseName, sql = s"CREATE INDEX $indexName ON $tableName ($indexColumn)"))
      logger.info(f"Created index '$indexName' in $indexCreationTime%.2f msec")

      // retrieve the row
      val results = service.executeQuery(databaseName, sql = s"SELECT * FROM $tableName WHERE $indexColumn = '$searchSymbol'")
      assert(results.rows.nonEmpty)
      for {
        row <- results.rows
      } {
        row.zipWithIndex foreach { case (result, index) => logger.info(s"[$index] $result") }
      }
    }

  }

  def copyInto(databaseName: String, tableName: String, file: File): Unit = {
    TableFile(databaseName, tableName) use { table =>
      val count = table.load(file)(_.split("[,]") match {
        case Array(symbol, exchange, price, date) =>
          Map("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> new Date(date.toLong))
        case _ => Map.empty
      })
      logger.info(s"Loaded $count items")
    }
  }

  def showBuffer(rowID: ROWID, buf: ByteBuffer)(implicit indexDevice: BlockDevice): Unit = {
    val row = Map(indexDevice.columns.zipWithIndex flatMap { case (column, idx) =>
      buf.position(indexDevice.columnOffsets(idx))
      val (_, value_?) = Codec.decode(column, buf)
      value_?.map(value => column.name -> value)
    }: _*)
    logger.debug(f"$rowID%d - $row")
  }

}
