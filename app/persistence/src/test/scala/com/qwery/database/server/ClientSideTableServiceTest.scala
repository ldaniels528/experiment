package com.qwery.database.server

import java.io.File
import java.util.Date

import akka.actor.ActorSystem
import com.qwery.database.models.TableCreation
import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, QueryProcessor, TupleSet, time}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

/**
 * Client-Side Table Service Test Suite
 */
class ClientSideTableServiceTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12120

  // start the server
  startServer(port)

  describe(classOf[ClientSideTableService].getName) {
    // create the client
    val service = ClientSideTableService(port = port)
    val databaseName = DEFAULT_DATABASE
    val tableNameA = "stocks_client_test_0"
    val tableNameB = "stocks_client_test_1"

    it("should drop an existing table") {
      invoke(label = s"service.dropTable($databaseName, $tableNameA)", service.dropTable(databaseName, tableNameA))
    }

    it("should drop an existing table (SQL)") {
      val sql = s"DROP TABLE $tableNameB"
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should create a new table") {
      val columns = Seq(
        Column(name = "symbol", comment = "the ticker symbol", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "exchange", comment = "the stock exchange", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(8)),
        Column(name = "lastSale", comment = "the latest sale price", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DoubleType)),
        Column(name = "lastTradeTime", comment = "the latest sale date/time", enumValues = Nil, ColumnMetadata(`type` = ColumnTypes.DateType))
      )
      invoke(
        label = s"service.createTable($databaseName, $tableNameA, $columns)",
        block = service.createTable(databaseName, TableCreation.create(tableNameA, columns)))
    }

    it("should create a new table (SQL)") {
      val sql =
        s"""|CREATE TABLE $tableNameB (
            |  symbol STRING(8) comment 'the ticker symbol',
            |  exchange STRING(8) comment 'the stock exchange',
            |  lastSale DOUBLE comment 'the latest sale price',
            |  lastTradeTime DATE comment 'the latest sale date/time'
            |)
            |LOCATION '/$databaseName/$tableNameB/'
            |""".stripMargin.trim
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should append a record to the end of a table") {
      val record = TupleSet("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"service.appendRow($databaseName, $tableNameA, $record)", service.insertRow(databaseName, tableNameA, record))
    }

    it("should replace a record at a specfic index") {
      val record = TupleSet("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"service.replaceRow($databaseName, $tableNameA,  rowID = 1, $record)", service.replaceRow(databaseName, tableNameA, rowID = 1, values = record))
    }

    it("should update a record at a specfic index") {
      val record = TupleSet("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 56.78, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"service.updateRow($databaseName, $tableNameA,  rowID = 1, $record)", service.updateRow(databaseName, tableNameA, rowID = 1, values = record))
    }

    it("should append a record to the end of a table (SQL)") {
      val sql =
        s"""|INSERT INTO $tableNameB (symbol, exchange, lastSale, lastTradeTime)
            |VALUES ("MSFT", "NYSE", 123.55, ${System.currentTimeMillis()})
            |""".stripMargin.replaceAllLiterally("\n", " ").trim
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should retrieve a field from a row on the server") {
      val (rowID, columnID) = (0, 0)
      invoke(
        label = s"service.getField($databaseName, $tableNameA, rowID = $rowID, columnID = $columnID)",
        block = service.getField(databaseName, tableNameA, rowID, columnID)
      )
    }

    it("should iterate records from the server") {
      invoke(
        label = s"service.toIterator($databaseName, $tableNameA)",
        block = service.toIterator(databaseName, tableNameA))
    }

    it("should bulk load data from a file") {
      copyInto(databaseName, tableNameA, new File("./stocks.csv"))
    }

    it("should bulk load data from a file (SQL)") {
      copyInto(databaseName, tableNameB, new File("./stocks.csv"))
    }

    it("should retrieve database metrics for a table from the server") {
      invoke(s"service.getDatabaseMetrics($databaseName)", service.getDatabaseMetrics(databaseName))
    }

    it("should retrieve table metrics for a table from the server") {
      invoke(s"service.getTableMetrics($databaseName, $tableNameA)", service.getTableMetrics(databaseName, tableNameA))
    }

    it("should retrieve a row by ID from the server") {
      invoke(s"service.getRow($databaseName, $tableNameA, rowID = 0)", service.getRow(databaseName, tableNameA, rowID = 0))
    }

    it("should retrieve a range of rows from the server") {
      invoke(
        label = s"service.getRange($databaseName, $tableNameA, start = 1000, length = 5)",
        block = service.getRange(databaseName, tableNameA, start = 1000, length = 5))
    }

    it("should search for a row via criteria from the server") {
      val condition = TupleSet("symbol" -> "MSFT")
      invoke(
        label = s"service.findRows($databaseName, $tableNameA, $condition, limit = Some(5))",
        block = service.findRows(databaseName, tableNameA, condition, limit = Some(5)))
    }

    it("should search for rows via criteria from the server") {
      val limit = Some(5)
      val condition = TupleSet("exchange" -> "NASDAQ")
      invoke(
        label = s"service.findRows($databaseName, $tableNameA, $condition, $limit)",
        block = service.findRows(databaseName, tableNameA, condition, limit))
    }

    it("should execute queries against the server") {
      val sql = s"SELECT * FROM $tableNameB WHERE symbol = 'AAPL'"
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should delete a row by ID from the server") {
      invoke(
        label = s"service.deleteRow($databaseName, $tableNameA, rowID = 999)",
        block = service.deleteRow(databaseName, tableNameA, rowID = 999))
    }

  }

  def copyInto(databaseName: String, tableName: String, file: File): Unit = {
    invoke(
      label = s"copyInto($databaseName, $tableName, ${file.getName})",
      block = TableFile(databaseName, tableName) use { table =>
        table.load(file)(_.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            TupleSet("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastTradeTime" -> new Date(date.toLong))
          case _ => TupleSet()
        })
      })
  }

  def invoke[A](label: String, block: => A): A = {
    val (results, responseTime) = time(block)
    results match {
      case it: Iterator[_] =>
        val rows = it.toList
        logger.info(f"$label ~> (${rows.size} items) [$responseTime%.1f msec]")
        rows.zipWithIndex.foreach { case (row, index) => logger.info(f"[$index%02d] $row") }
      case rows: Seq[_] =>
        logger.info(f"$label ~> (${rows.size} items) [$responseTime%.1f msec]")
        rows.zipWithIndex.foreach { case (row, index) => logger.info(f"[$index%02d] $row") }
      case result => logger.info(f"$label ~> $result [$responseTime%.1f msec]")
    }
    results
  }

  def startServer(port: Int): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    implicit val queryProcessor: QueryProcessor = new QueryProcessor(routingActors = 5, requestTimeout = 5.seconds)
    import system.dispatcher

    logger.info(s"Starting Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

}
