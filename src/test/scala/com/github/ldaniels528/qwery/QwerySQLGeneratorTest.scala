package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.QwerySQLGenerator._
import com.github.ldaniels528.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Qwery SQL Generator Test
  * @author lawrence.daniels@gmail.com
  */
class QwerySQLGeneratorTest extends FunSpec {

  it("supports DESCRIBE") {
    val sql0 = "DESCRIBE 'companylist.csv'"
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("supports DESCRIBE w/LIMIT") {
    val sql0 = "DESCRIBE 'companylist.csv' LIMIT 100"
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("supports DESCRIBE-SELECT") {
    val sql0 = "DESCRIBE (SELECT ymbol, Name, Sector, Industry FROM 'companylist.csv')"
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("supports SELECT-WHERE-LIMIT") {
    val sql0 =
      """
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Consumer Specialties'
        |LIMIT 25""".stripMargin.toSingleLine
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("supports SELECT-TOP-WHERE") {
    val sql0 =
      """
        |SELECT TOP 25 Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Consumer Specialties' AND Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
        |""".stripMargin.toSingleLine
    val sql1 =
      """
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Consumer Specialties' AND Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
        |LIMIT 25""".stripMargin.toSingleLine
    val sql2 = QweryCompiler(sql0).toSQL
    assert(sql1 == sql2)
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
      """.stripMargin.toSingleLine
    val sql1 =
      """
        |SELECT
        |   CASE
        |     WHEN 'Hello World' = 'HelloWorld' THEN 'Found 1'
        |     WHEN 'Hello World' = 'Hello' || ' ' || 'World' THEN 'Found 2'
        |     ELSE 'Not Found'
        |   END
      """.stripMargin.toSingleLine
    val sql2 = QweryCompiler(sql0).toSQL
    assert(sql1 == sql2)
  }

  it("supports INSERT-INTO-SELECT-WITH-FORMAT") {
    val sql0 =
      """
        |INSERT INTO 'companylist.json' (Symbol, Name, Sector, Industry) WITH FORMAT JSON
        |SELECT Symbol, Name, Sector, Industry, `Summary Quote`
        |FROM 'companylist.csv' WITH FORMAT CSV
        |WHERE Industry = 'Oil/Gas Transmission'
      """.stripMargin.toSingleLine
    val sql2 = QweryCompiler(sql0).toSQL
    info(sql2)
  }

  it("supports INSERT-INTO-SELECT") {
    val sql0 =
      """
        |INSERT INTO 'test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Consumer Specialties' OR Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'
        |LIMIT 1000""".stripMargin.toSingleLine
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("supports INSERT-OVERWRITE-SELECT") {
    val sql0 =
      """
        |INSERT OVERWRITE 'test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'""".stripMargin.toSingleLine
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

}
