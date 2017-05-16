package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.QwerySQLGenerator._
import com.github.ldaniels528.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * Qwery SQL Generator Test
  * @author lawrence.daniels@gmail.com
  */
class QwerySQLGeneratorTest extends FunSpec {

  it("DESCRIBE") {
    val sql0 = "DESCRIBE 'companylist.csv'"
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("DESCRIBE w/LIMIT") {
    val sql0 = "DESCRIBE 'companylist.csv' LIMIT 100"
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("SELECT-WHERE-LIMIT") {
    val sql0 =
      """
        |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
        |FROM 'companylist.csv'
        |WHERE Industry = 'Consumer Specialties'
        |LIMIT 25""".stripMargin.toSingleLine
    val sql1 = QweryCompiler(sql0).toSQL
    assert(sql0 == sql1)
  }

  it("SELECT-TOP-WHERE") {
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

  it("SELECT-CASE-WHEN") {
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

  it("INSERT-INTO-SELECT") {
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

  it("INSERT-OVERWRITE-SELECT") {
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
