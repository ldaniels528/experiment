package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.QweryDecompiler._
import com.github.ldaniels528.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Qwery Decompiler Test
  * @author lawrence.daniels@gmail.com
  */
class QweryDecompilerTest extends FunSpec {

  describe("QweryDecompiler") {

    it("should decompile CREATE VIEW statements") {
      val sql =
        """
          |CREATE VIEW OilAndGas AS
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin.toSingleLine
      assert(QweryCompiler(sql).toSQL == sql)
    }

    it("should decompile variable assignments") {
      val sql = "SET @x = 1.0"
      assert(QweryCompiler(sql).toSQL == sql)
    }

    it("should decompile variable declarations") {
      val sql = "DECLARE @counter DOUBLE"
      assert(QweryCompiler(sql).toSQL == sql)
    }

    it("supports DESCRIBE") {
      val sql = "DESCRIBE 'companylist.csv'"
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports DESCRIBE w/LIMIT") {
      val sql = "DESCRIBE 'companylist.csv' LIMIT 100"
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports DESCRIBE-SELECT") {
      val sql = "DESCRIBE (SELECT ymbol, Name, Sector, Industry FROM 'companylist.csv')"
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports SELECT-INTO-WHERE-LIMIT") {
      val sql0 =
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |INTO 'companylist.json'
          |FROM 'companylist.csv'
          |WHERE Industry = 'Consumer Specialties'
          |LIMIT 25""".stripMargin.toSingleLine
      val sql1 =
        """
          |INSERT INTO 'companylist.json' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Industry = 'Consumer Specialties'
          |LIMIT 25""".stripMargin.toSingleLine
      assert(sql1 == QweryCompiler(sql0).toSQL)
    }

    it("supports SELECT-WHERE-LIMIT") {
      val sql =
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Industry = 'Consumer Specialties'
          |LIMIT 25""".stripMargin.toSingleLine
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports SELECT-SUB-SELECT") {
      val sql =
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM (SELECT * FROM 'companylist.csv' WHERE Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)')
          |WHERE Sector = 'Consumer Specialties'
          |LIMIT 25""".stripMargin.toSingleLine
      assert(
        sql == QweryCompiler(sql).toSQL)
    }

    it("supports SELECT-TOP-WHERE") {
      val sql0 =
        """
          |SELECT TOP 25 Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Sector = 'Consumer Specialties' AND Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
          |""".stripMargin.toSingleLine
      val sql1 =
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Sector = 'Consumer Specialties' AND Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
          |LIMIT 25""".stripMargin.toSingleLine
      assert(
        sql1 == QweryCompiler(sql0).toSQL)
    }

    it("supports SELECT-CASE-WHEN") {
      val sql0 =
        """
          |SELECT
          |   CASE 'Hello World'
          |     WHEN 'HelloWorld' THEN 'Found 1'
          |     WHEN 'Hello' || ' ' || 'World' THEN 'Found 2'
          |     ELSE 'Not Found'
          |   END
        """.
          stripMargin.toSingleLine
      val sql1 =
        """
          |SELECT
          |   CASE
          |     WHEN 'Hello World' = 'HelloWorld' THEN 'Found 1'
          |     WHEN 'Hello World' = 'Hello' || ' ' || 'World' THEN 'Found 2'
          |     ELSE 'Not Found'
          |   END
        """.
          stripMargin.toSingleLine
      assert(sql1 == QweryCompiler(sql0).toSQL)
    }

    it("supports INSERT-INTO-SELECT-WITH-FORMATS") {
      val sql0 =
        """
          |INSERT INTO 'companylist.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv' WITH CSV FORMAT
          |WHERE Industry = 'Oil/Gas Transmission'
          |""".stripMargin.toSingleLine
      val sql1 =
        """
          |INSERT INTO 'companylist.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WITH DELIMITER ','
          |WITH COLUMN HEADERS
          |WITH QUOTED TEXT
          |WHERE Industry = 'Oil/Gas Transmission'
          |""".
          stripMargin.toSingleLine
      assert(sql1 == QweryCompiler(sql0).toSQL)
    }

    it("supports INSERT-INTO-SELECT-WITH-MULTIPLE") {
      val sql =
        """
          |INSERT INTO 'companylist.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
          |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
          |FROM 'companylist.csv'
          |WITH DELIMITER ','
          |WITH COLUMN HEADERS
          |WITH QUOTED NUMBERS
          |WITH QUOTED TEXT
          |WHERE Industry = 'Oil/Gas Transmission'
          |""".
          stripMargin.toSingleLine
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports INSERT-INTO-SELECT") {
      val sql =
        """
          |INSERT INTO 'test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Industry = 'Consumer Specialties' OR Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
          |LIMIT 1000""".stripMargin.toSingleLine
      assert(sql == QweryCompiler(sql).toSQL)
    }

    it("supports INSERT-OVERWRITE-SELECT") {
      val sql =
        """
          |INSERT OVERWRITE 'test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM 'companylist.csv'
          |WHERE Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'""".stripMargin.toSingleLine
      assert(sql == QweryCompiler(sql).toSQL)
    }

  }

}
