package com.qwery.database
package jdbc

import com.qwery.database.server.testkit.DatabaseTestServer
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.sql.{DriverManager, ResultSet, ResultSetMetaData}

/**
 * Qwery JDBC Driver Test Suite
 */
class QweryDriverTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12121
  private val jdbcURL = s"jdbc:qwery://localhost:$port/test"
  private val tableNameA = "stocks_jdbc"
  private val tableNameB = "stocks_jdbc_columnar"

  // start the server
  DatabaseTestServer.startServer(port)

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
        iterateRows(conn.getMetaData.getTables( "test", null, null, null)) { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should retrieve the columns for a given database") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        iterateRows(conn.getMetaData.getColumns( "test", null, "stock", "symbol")) { row =>
          logger.info(s"row: $row")
        }
      }
    }

    it("should execute a DROP TABLE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(s"DROP TABLE IF EXISTS $tableNameA")
        assert(count >= 0 && count <= 1)
      }
    }

    it("should execute a CREATE TABLE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|CREATE TABLE $tableNameA (
              |  symbol STRING(8) comment 'the ticker symbol',
              |  exchange STRING(8) AS ENUM ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC') comment 'the stock exchange',
              |  lastSale DOUBLE comment 'the latest sale price',
              |  lastTradeTime DATE comment 'the latest sale date/time'
              |)
              |WITH DESCRIPTION 'securities table'
              |""".stripMargin
        )
        assert(count == 1)
      }
    }

    it("should execute an INSERT statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|INSERT INTO $tableNameA (symbol, exchange, lastSale, lastTradeTime)
              |VALUES ("MSFT", "NYSE", 56.55, now()), ("AAPL", "NASDAQ", 98.55, now())
              |""".stripMargin
        )
        assert(count == 2)
      }
    }

    it("should execute an INSERT statement with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val now = System.currentTimeMillis()
        val ps = conn.prepareStatement(
          s"""|INSERT INTO $tableNameA (symbol, exchange, lastSale, lastTradeTime)
              |VALUES (?, ?, ?, ?), (?, ?, ?, ?)
              |""".stripMargin
        )
        Seq("AMZN", "NYSE", 56.55, now, "GOOG", "NASDAQ", 98.55, now).zipWithIndex foreach { case (value, index) =>
          ps.setObject(index + 1, value)
        }
        val count = ps.executeUpdate()
        assert(count == 2)
      }
    }

    it("should execute an UPDATE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|UPDATE $tableNameA SET lastSale = 101.12, lastTradeTime = now() WHERE symbol = "GOOG"
              |""".stripMargin
        )
        assert(count == 1)
      }
    }

    it("should execute a SELECT query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA WHERE symbol = 'AAPL'") use { rs =>
          rs.next()
          assert(rs.getString("symbol") == "AAPL")
        }
      }
    }

    it("should execute a SELECT query with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.prepareStatement(s"SELECT * FROM $tableNameA WHERE symbol = ?") use { ps =>
          ps.setString(1, "AAPL")
          ps.executeQuery() use { rs =>
            rs.next()
            assert(rs.getString("symbol") == "AAPL")
          }
        }
      }
    }

    it("should execute a COUNT query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|SELECT COUNT(*) FROM $tableNameA
              |""".stripMargin) use { rs =>
          rs.next()
          assert(rs.getLong(1) == 4)
        }
      }
    }

    it("should execute a COUNT query with alias") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|SELECT COUNT(*) AS transactions FROM $tableNameA
              |""".stripMargin) use { rs =>
          rs.next()
          assert(rs.getLong("transactions") == 4)
        }
      }
    }

    it("should execute a summarization query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|SELECT
              |   COUNT(*) AS transactions,
              |   AVG(lastSale) AS avgLastSale,
              |   MIN(lastSale) AS minLastSale,
              |   MAX(lastSale) AS maxLastSale,
              |   SUM(lastSale) AS sumLastSale
              |FROM $tableNameA
              |""".stripMargin) use { rs =>
          rs.next()
          assert(rs.getDouble("sumLastSale") == 312.77)
          assert(rs.getDouble("maxLastSale") == 101.12)
          assert(rs.getDouble("avgLastSale") == 78.1925)
          assert(rs.getDouble("minLastSale") == 56.55)
          assert(rs.getInt("transactions") == 4)
        }
      }
    }

    it("should modify and insert a new record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA LIMIT 20") use { rs0 =>
          rs0.next()
          rs0.updateString("symbol", "CCC")
          rs0.updateDouble("lastSale", 15.44)
          rs0.insertRow()
        }

        // retrieve the row again
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA WHERE symbol = 'CCC'") use { rs1 =>
          rs1.next()
          assert(rs1.getString("symbol") == "CCC")
          assert(rs1.getDouble("lastSale") == 15.44)
        }
      }
    }

    it("should modify and update an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA LIMIT 20") use { rs0 =>
          rs0.next()
          rs0.updateString("symbol", "AAA")
          rs0.updateDouble("lastSale", 78.99)
          rs0.updateLong("lastTradeTime", System.currentTimeMillis())
          rs0.updateRow()
        }

        // retrieve the row again
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA WHERE symbol = 'AAA'") use { rs1 =>
          rs1.next()
          assert(rs1.getString("symbol") == "AAA")
          assert(rs1.getDouble("lastSale") == 78.99)
        }
      }
    }

    it("should delete an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA WHERE symbol = 'CCC'") use { rs0 =>
          rs0.next()
          rs0.deleteRow()
        }

        // retrieve the row again
        conn.createStatement().executeQuery(s"SELECT * FROM $tableNameA WHERE symbol = 'CCC'") use { rs1 =>
          assert(!rs1.next())
        }
      }
    }

    it("should execute a CREATE COLUMNAR TABLE statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // drop the existing table
        conn.createStatement().execute(s"DROP TABLE IF EXISTS $tableNameB")

        // create a new table
        val count = conn.createStatement().executeUpdate(
          s"""|CREATE COLUMNAR TABLE $tableNameB (
              |  symbol STRING(8) comment 'the ticker symbol',
              |  exchange STRING(8) AS ENUM ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC') comment 'the stock exchange',
              |  lastSale DOUBLE comment 'the latest sale price',
              |  lastTradeTime DATE comment 'the latest sale date/time'
              |)
              |WITH DESCRIPTION 'securities table (columnar)'
              |""".stripMargin
        )
        assert(count == 1)
      }
    }

    it("should insert records into a columnar table") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|INSERT INTO $tableNameB (symbol, exchange, lastSale, lastTradeTime)
              |VALUES ("MSFT", "NYSE", 56.55, now()), ("AAPL", "NASDAQ", 98.55, now())
              |""".stripMargin
        )
        assert(count == 2)
      }
    }

  }

  def __id(rs: ResultSet): ROWID = ByteBuffer.wrap(rs.getRowId(1).getBytes).getInt

  def iterateRows(rs: ResultSet)(f: Map[String, Any] => Unit): Unit = {
    val rsmd = rs.getMetaData
    while (rs.next()) f(toRow(rs, rsmd))
  }

  def toRow(rs: ResultSet, rsmd: ResultSetMetaData): Map[String, AnyRef] = {
    Map((
      for (index <- 1 to rsmd.getColumnCount) yield {
        val name = rsmd.getColumnName(index)
        val value = rs.getObject(name)
        name -> value
      }): _*)
  }

}
