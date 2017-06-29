package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.ops.{Scope, Variable}
import org.scalatest.FunSpec

import scala.util.Properties

/**
  * JDBC Support Tests
  * @author lawrence.daniels@gmail.com
  */
class JDBCSupportTest() extends FunSpec {
  private val scope = Scope.root()

  describe("JDBCSupport") {

    it("should execute native SQL updates") {
      val sql =
        """
          |NATIVE SQL 'TRUNCATE TABLE company'
          |FROM 'jdbc:mysql://localhost:3306/test'
          |WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
        """.stripMargin

      ifDbSet { () =>
        val resultSet = QweryCompiler(sql).execute(scope)
        assert(resultSet.rows.toSeq == Seq(Seq(("ROWS_AFFECTED", 0))))
      }
    }

    it("should insert records into a database") {
      /*
      DROP TABLE company;
      CREATE TABLE company (
        Symbol VARCHAR(10) PRIMARY KEY,
        Name VARCHAR(64),
        LastSale DECIMAL(9, 4),
        MarketCap DECIMAL(20,4),
        Sector VARCHAR(64),
        Industry VARCHAR(64));
     */
      val sql =
        """
          |INSERT INTO 'jdbc:mysql://localhost:3306/test?table=company' (Symbol, Name, Sector, Industry, MarketCap, LastSale)
          |SELECT Symbol, Name, Sector, Industry, MarketCap,
          | CASE LastSale
          |   WHEN 'n/a' THEN NULL
          |   ELSE CAST(LastSale AS DOUBLE)
          | END
          |FROM './companylist.csv'
          |WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
        """.stripMargin

      ifDbSet { () =>
        val resultSet = QweryCompiler(sql).execute(scope)
        assert(resultSet.rows.toSeq == Seq(Seq(("ROWS_INSERTED", 359))))
      }
    }

    it("should execute native SQL queries") {
      scope += Variable("symbol", Some("UFAB"))
      val sql =
        """
          |NATIVE SQL 'SELECT * FROM company WHERE Symbol = "{{ symbol }}"'
          |FROM 'jdbc:mysql://localhost:3306/test'
          |WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
        """.stripMargin

      ifDbSet { () =>
        val resultSet = QweryCompiler(sql).execute(scope)
        assert(resultSet.rows.toSeq ==
          Stream(Vector(
            "Symbol" -> "UFAB", "Name" -> "Unique Fabricating, Inc.", "LastSale" -> new java.math.BigDecimal("11.6100"),
            "MarketCap" -> new java.math.BigDecimal("113251010.4900"), "Sector" -> "Capital Goods",
            "Industry" -> "Auto Parts:O.E.M."
          )))
      }
    }

    it("should retrieve records from a database") {
      val sql =
        """
          |INSERT OVERWRITE './companyinfo.txt' (Symbol^10, Name^64, Sector^64, Industry^64, MarketCap^10, LastSale^10)
          |WITH FIXED WIDTH
          |SELECT * FROM 'jdbc:mysql://localhost:3306/test?table=company'
          |WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
        """.stripMargin

      ifDbSet { () =>
        val resultSet = QweryCompiler(sql).execute(scope)
        assert(resultSet.rows.toSeq == Seq(Seq(("ROWS_INSERTED", 359))))
      }
    }

  }

  def ifDbSet[S](block: () => S): Unit = {
    Properties.envOrNone("QWERY_JDBC") match {
      case Some(_) => block()
      case None =>
        alert("Set 'QWERY_JDBC' to test JDBC I/O")
    }
  }

}
