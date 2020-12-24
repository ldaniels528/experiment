package com.qwery.database
package jdbc

import java.nio.ByteBuffer
import java.sql.{DriverManager, ResultSet, ResultSetMetaData}

import akka.actor.ActorSystem
import com.qwery.database.server.DatabaseServer
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt

/**
 * Qwery JDBC Driver Test Suite
 */
class QweryDriverTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12121
  private val jdbcURL = s"jdbc:qwery://localhost:$port/test"
  private val tableName = "stocks_test_jdbc"

  // start the server
  startServer(port)

  describe(QweryDriver.getClass.getSimpleName) {
    Class.forName("com.qwery.database.jdbc.QweryDriver")

    it("should provide basic JDBC driver information") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val dmd = conn.getMetaData
        logger.info(s"Driver Name:        ${dmd.getDriverName}")
        logger.info(s"Product Name:       ${dmd.getDatabaseProductName}")
        logger.info(s"JDBC Major Version: ${dmd.getJDBCMajorVersion}")
        logger.info(s"JDBC Minor Version: ${dmd.getJDBCMinorVersion}")
      }
    }

    it("should retrieve the data type information") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        iterateRows(conn.getMetaData.getTypeInfo)(row => logger.info(s"row: $row"))
      }
    }

    it("should retrieve the list of databases") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        iterateRows(conn.getMetaData.getCatalogs)(row => logger.info(s"row: $row"))
      }
    }

    it("should retrieve the tables for a given database") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        iterateRows(conn.getMetaData.getTables( "test", null, null, null))(row => logger.info(s"row: $row"))
      }
    }

    it("should execute a DROP TABLE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sql = s"DROP TABLE IF EXISTS $tableName"
        val isDropped = conn.createStatement().execute(sql)
        logger.info(f"$sql => $isDropped")
        //assert(isDropped)
      }
    }

    it("should execute a CREATE TABLE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sql =
          s"""|CREATE TABLE $tableName (
              |  symbol STRING(8) comment 'the ticker symbol',
              |  exchange STRING AS ENUM ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC') comment 'the stock exchange',
              |  lastSale DOUBLE comment 'the latest sale price',
              |  lastTradeTime DATE comment 'the latest sale date/time'
              |)
              |LOCATION 'qwery://test'
              |""".stripMargin
        val isCreated = conn.createStatement().execute(sql)
        logger.info(f"$sql => $isCreated")
        assert(isCreated)
      }
    }

    it("should execute an INSERT statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val now = System.currentTimeMillis()
        val sql =
          s"""|INSERT INTO $tableName (symbol, exchange, lastSale, lastTradeTime)
              |VALUES ("MSFT", "NYSE", 56.55, $now), ("AAPL", "NASDAQ", 98.55, $now)
              |""".stripMargin
        val count = conn.createStatement().executeUpdate(sql)
        logger.info(f"Insert count: $count")
        assert(count == 2)
      }
    }

    it("should execute an INSERT statement with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val now = System.currentTimeMillis()
        val sql =
          s"""|INSERT INTO $tableName (symbol, exchange, lastSale, lastTradeTime)
              |VALUES (?, ?, ?, ?), (?, ?, ?, ?)
              |""".stripMargin
        val ps = conn.prepareStatement(sql)
        Seq("AMZN", "NYSE", 56.55, now, "GOOG", "NASDAQ", 98.55, now).zipWithIndex foreach { case (value, index) =>
          ps.setObject(index + 1, value)
        }
        val count = ps.executeUpdate()
        logger.info(f"Insert count: $count")
        assert(count == 2)
      }
    }

    it("should execute an UPDATE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val now = System.currentTimeMillis()
        val sql =
          s"""|UPDATE $tableName SET lastSale = 101.12, lastTradeTime = $now WHERE symbol = "GOOG"
              |""".stripMargin
        val count = conn.createStatement().executeUpdate(sql)
        logger.info(f"Update count: $count")
        assert(count == 1)
      }
    }

    it("should execute a SELECT query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(s"SELECT * FROM $tableName WHERE symbol = 'AAPL'")
        iterateRows(rs)(row => logger.info(f"row [${__id(rs)}%02d]: $row"))
      }
    }

    it("should execute a SELECT query with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(s"SELECT * FROM $tableName WHERE symbol = ?")
        ps.setString(1, "AAPL")
        val rs = ps.executeQuery()
        iterateRows(rs)(row => logger.info(f"row [${__id(rs)}%02d]: $row"))
      }
    }

    it("should modify and insert a new record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs0 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        rs0.next()
        rs0.updateDouble("lastSale", 15.44)
        rs0.insertRow()

        // retrieve the rows again
        val rs1 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        iterateRows(rs1)(row => logger.info(f"row [${__id(rs1)}%02d]: $row"))
      }
    }

    it("should modify and update an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs0 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        rs0.next()
        rs0.updateString("symbol", "AAA")
        rs0.updateDouble("lastSale", 78.99)
        rs0.updateLong("lastTradeTime", System.currentTimeMillis())
        rs0.updateRow()

        // retrieve the rows again
        val rs1 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        iterateRows(rs1)(row => logger.info(f"row [${__id(rs1)}%02d]: $row"))
      }
    }

    it("should delete an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs0 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        rs0.next()
        rs0.deleteRow()

        // retrieve the rows again
        val rs1 = conn.createStatement().executeQuery(s"SELECT * FROM $tableName LIMIT 20")
        iterateRows(rs1)(row => logger.info(f"row [${__id(rs1)}%02d]: $row"))
      }
    }

  }

  def __id(rs: ResultSet): ROWID = ByteBuffer.wrap(rs.getRowId(1).getBytes).getInt

  def iterateRows(rs: ResultSet)(f: Map[String, Any] => Unit): Unit = {
    val rsmd = rs.getMetaData
    while (rs.next()) f(toRow(rs, rsmd))
  }

  def startServer(port: Int): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    implicit val queryProcessor: QueryProcessor = new QueryProcessor(requestTimeout = 5.seconds)
    import system.dispatcher

    logger.info(s"Starting Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

  def toRow(rs: ResultSet, rsmd: ResultSetMetaData): Map[String, AnyRef] = {
    Map((
      for (index <- 1 to rsmd.getColumnCount) yield {
        val name = rsmd.getColumnName(index)
        val value = rs.getObject(index)
        name -> value
      }): _*)
  }

}
