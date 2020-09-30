package com.qwery.database.server

import akka.actor.ActorSystem
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

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
    val theClient = ClientSideTableService(port = port)
    val databaseName = DEFAULT_DATABASE

    it("should drop an existing table") {
      theClient.dropTable(databaseName, tableName = "stocks_test")
    }

    it("should create a new table") {
      theClient.executeQuery(databaseName, sql =
        """|create table stocks_test (
           |  symbol String(8),
           |  exchange String(8),
           |  lastSale Double,
           |  lastSaleTime Long
           |)
           |location './qwery-db/customers/'
           |""".stripMargin
      )
    }

    it("should append a record to the end of a table") {
      val record = Map("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      val updateCount = theClient.appendRow(databaseName, tableName = "stocks_test", record)
      logger.info(s"updateCount: $updateCount")
    }

    it("should retrieve database metrics for a table from the server") {
      val metrics = theClient.getDatabaseMetrics(databaseName)
      logger.info(s"metrics: $metrics")
    }

    it("should retrieve table metrics for a table from the server") {
      val metrics = theClient.getTableMetrics(databaseName, tableName = "stocks_test")
      logger.info(s"metrics: $metrics")
    }

    it("should retrieve a row by ID from the server") {
      val row = theClient.getRow(databaseName, tableName = "stocks_test", rowID = 0)
      logger.info(s"row: $row")
    }

    it("should retrieve a range of rows from the server") {
      val rows = theClient.getRange(databaseName, tableName = "stocks_test", start = 1000, length = 5)
      rows.zipWithIndex.foreach { case (row,index) => logger.info(f"[$index%02d] $row") }
    }

    it("should search for a row via criteria from the server") {
      val rows = theClient.findRows(databaseName, tableName = "stocks_test", condition = Map("symbol" -> "MSFT"), limit = Some(5))
      rows.foreach(row => logger.info(row.toString()))
    }

    it("should search for rows via criteria from the server") {
      val rows = theClient.findRows(databaseName, tableName = "stocks_test", condition = Map("exchange" -> "NASDAQ"), limit = Some(5))
      rows.zipWithIndex.foreach { case (row,index) => logger.info(f"[$index%02d] $row") }
    }

    it("should execute queries against the server") {
      val rows = theClient.executeQuery(databaseName, "SELECT * FROM stocks_test WHERE symbol = 'AAPL'")
      rows.zipWithIndex.foreach { case (row,index) => logger.info(f"[$index%02d] $row") }
    }

    it("should delete a row by ID from the server") {
      val deletedCount = theClient.deleteRow(databaseName, tableName = "stocks_test", rowID = 999)
      logger.info(s"deleted: $deletedCount")
    }

  }

  def startServer(port: Int): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    implicit val service: ServerSideTableService = new ServerSideTableService()
    import system.dispatcher

    logger.info(s"Starting Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

}
