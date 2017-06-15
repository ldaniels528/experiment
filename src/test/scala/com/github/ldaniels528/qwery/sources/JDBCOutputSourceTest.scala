package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.QweryCompiler
import com.github.ldaniels528.qwery.ops.RootScope
import org.scalatest.FunSpec

/**
  * JDBC Output Source Test
  * @author lawrence.daniels@gmail.com
  */
class JDBCOutputSourceTest() extends FunSpec {
  private val scope = RootScope()

  describe("JDBCOutputSource") {

    /*
      DROP TABLE company;
      CREATE TABLE company (
        Symbol VARCHAR(10),
        Name VARCHAR(64),
        LastSale DECIMAL(9, 4),
        MarketCap DECIMAL(20,4),
        Sector VARCHAR(64),
        Industry VARCHAR(64)
      );
     */
    it("should insert records into a database using QQL") {
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
      val resultSet = QweryCompiler(sql).execute(scope)
      info(s"resultSet: $resultSet")
    }

  }

}
