package com.qwery.database
package clients

import com.qwery.database.files.TableFile
import com.qwery.database.models._
import com.qwery.database.server.testkit.DatabaseTestServer
import com.qwery.models.{ColumnSpec, Table, TableRef, Column => XColumn}
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Date

/**
 * Database Client Test Suite
 */
class DatabaseClientTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12120

  // start the server
  DatabaseTestServer.startServer(port)

  describe(classOf[DatabaseClient].getSimpleName) {
    // create the client
    val service = DatabaseClient(port = port)
    val databaseName = "test"
    val schemaName = "stocks"
    val tableNameA = "stocks_client_test_0"
    val tableNameB = "stocks_client_test_1"

    it("should search for databases on the server") {
      invoke(label = "service.searchDatabases(databaseNamePattern = \"t%\")",
        service.searchDatabases(databaseNamePattern = Some("t%")))
    }

    it("should search for schemas on the server") {
      invoke(label = "service.searchSchemas(databaseNamePattern = \"t%\", schemaNamePattern = \"s%\")",
        service.searchSchemas(databaseNamePattern = Some("t%"), schemaNamePattern = Some("s%")))
    }

    it("should search for tables on the server") {
      invoke(label = "service.searchTables(databaseNamePattern = \"t%\", schemaNamePattern = Some(\"s%\"), tableNamePattern = \"s%\")",
        service.searchTables(databaseNamePattern = Some("t%"), schemaNamePattern = Some("s%"), tableNamePattern = Some("s%")))
    }

    it("should search for columns on the server") {
      invoke(label = "service.searchColumns(databaseNamePattern = \"test\", schemaNamePattern = Some(\"s%\"), tableNamePattern = \"s%\", columnNamePattern = \"symbol\")",
        service.searchColumns(databaseNamePattern = Some("test"), schemaNamePattern = Some("s%"), tableNamePattern = Some("s%"), columnNamePattern = Some("symbol")))
    }

    it("should drop an existing table") {
      invoke(label = s"""service.dropTable("$databaseName", "$schemaName", "$tableNameA")""",
        service.dropTable(databaseName, schemaName, tableNameA, ifExists = true))
    }

    it("should drop an existing table (SQL)") {
      val sql = s"DROP TABLE IF EXISTS `$schemaName.$tableNameB`"
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should create a new table") {
      val columns = List(
        XColumn(name = "symbol", comment = Some("the ticker symbol"), spec = ColumnSpec(typeName = "String", precision = List(8))),
        XColumn(name = "exchange", comment = Some("the stock exchange"), spec = ColumnSpec(typeName = "String", precision = List(8))),
        XColumn(name = "lastSale", comment = Some("the latest sale price"), spec = ColumnSpec(typeName = "Double")),
        XColumn(name = "lastSaleTime", comment = Some("the latest sale date/time"), spec = ColumnSpec(typeName = "DateTime"))
      )
      invoke(
        label = s"service.createTable($databaseName, $schemaName, $tableNameA, $columns)",
        block = service.createTable(databaseName, schemaName, tableNameA, Table(
          ref = new TableRef(databaseName, schemaName, tableNameA),
          columns = columns,
          description = Some("API created table")))
      )
    }

    it("should create a new table (SQL)") {
      val sql =
        s"""|CREATE TABLE `$schemaName.$tableNameB` (
            |  symbol STRING(8) comment 'the ticker symbol',
            |  exchange STRING(8) comment 'the stock exchange',
            |  lastSale DOUBLE comment 'the latest sale price',
            |  lastSaleTime DATE comment 'the latest sale date/time'
            |) WITH COMMENT 'SQL created table'
            |""".stripMargin.trim
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should append a record to the end of a table") {
      val record = KeyValues("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.appendRow("$databaseName", "$schemaName", "$tableNameA", $record)""", service.insertRow(databaseName, schemaName, tableNameA, record))
    }

    it("should replace a record at a specfic index") {
      val record = KeyValues("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.replaceRow("$databaseName", "$schemaName", "$tableNameA", rowID = 1, $record)""", service.replaceRow(databaseName, schemaName, tableNameA, rowID = 1, values = record))
    }

    it("should update a record at a specfic index") {
      val record = KeyValues("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 56.78, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.updateRow("$databaseName", "$schemaName", "$tableNameA", rowID = 1, $record)""", service.updateRow(databaseName, schemaName, tableNameA, rowID = 1, values = record))
    }

    it("should append a record to the end of a table (SQL)") {
      val sql =
        s"""|INSERT INTO `$schemaName.$tableNameB` (symbol, exchange, lastSale, lastSaleTime)
            |VALUES ("MSFT", "NYSE", 123.55, now())
            |""".stripMargin.replaceAllLiterally("\n", " ").trim
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should retrieve a field from a row on the server") {
      val (rowID, columnID) = (0, 0)
      invoke(
        label = s"""service.getField("$databaseName", "$schemaName", "$tableNameA", rowID = $rowID, columnID = $columnID)""",
        block = service.getFieldAsBytes(databaseName, schemaName, tableNameA, rowID, columnID)
      )
    }

    it("should iterate records from the server") {
      invoke(
        label = s"""service.toIterator("$databaseName", "$schemaName", "$tableNameA")""",
        block = service.toIterator(databaseName, schemaName, tableNameA))
    }

    it("should bulk load data from a file") {
      copyInto(databaseName, schemaName, tableNameA, new File("./stocks.csv"))
    }

    it("should bulk load data from a file (SQL)") {
      copyInto(databaseName, schemaName, tableNameB, new File("./stocks.csv"))
    }

    it("should retrieve database summary for a table from the server") {
      invoke(s"""service.getDatabaseSummary("$databaseName")""", service.getDatabaseSummary(databaseName))
    }

    it("should retrieve table metrics for a table from the server") {
      invoke(s"""service.getTableMetrics("$databaseName", "$schemaName", "$tableNameA")""", service.getTableMetrics(databaseName, schemaName, tableNameA))
    }

    it("should retrieve a row by ID from the server") {
      invoke(s"""service.getRow("$databaseName", "$schemaName", "$tableNameA", rowID = 0)""", service.getRow(databaseName, schemaName, tableNameA, rowID = 0))
    }

    it("should retrieve a range of rows from the server") {
      invoke(
        label = s"""service.getRange("$databaseName", "$schemaName", "$tableNameA", start = 1000, length = 5)""",
        block = service.getRange(databaseName, schemaName, tableNameA, start = 1000, length = 5))
    }

    it("should search for a row via criteria from the server") {
      val condition = KeyValues("symbol" -> "MSFT")
      invoke(
        label = s"""service.findRows("$databaseName", "$schemaName", "$tableNameA", $condition, limit = Some(5))""",
        block = service.findRows(databaseName, schemaName, tableNameA, condition, limit = Some(5)))
    }

    it("should search for rows via criteria from the server") {
      val limit = Some(5)
      val condition = KeyValues("exchange" -> "NASDAQ")
      invoke(
        label = s"""service.findRows("$databaseName", "$schemaName", "$tableNameA", $condition, $limit)""",
        block = service.findRows(databaseName, schemaName, tableNameA, condition, limit))
    }

    it("should execute queries against the server") {
      val sql = s"SELECT * FROM `$schemaName.$tableNameB` WHERE exchange = 'NASDAQ' LIMIT 5"
      invoke(sql, service.executeQuery(databaseName, sql))
    }

    it("should delete a row by ID from the server") {
      invoke(
        label = s"""service.deleteRow("$databaseName", "$schemaName", "$tableNameA", rowID = 999)""",
        block = service.deleteRow(databaseName, schemaName, tableNameA, rowID = 999))
    }

  }

  def copyInto(databaseName: String, schemaName: String, tableName: String, file: File): Unit = {
    invoke(
      label = s"copyInto($databaseName, $schemaName, $tableName, ${file.getName})",
      block = TableFile(new TableRef(databaseName, schemaName, tableName)) use { table =>
        table.ingestTextFile(file)(_.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            Option(KeyValues("symbol" -> symbol, "exchange" -> exchange, "lastSale" -> price.toDouble, "lastSaleTime" -> new Date(date.toLong)))
          case _ => None
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
      case results: QueryResult =>
        results.show()(logger)
      case result =>
        logger.info(f"$label ~> $result [$responseTime%.1f msec]")
    }
    results
  }

}
