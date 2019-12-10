package com.qwery.language

import org.scalatest.funspec.AnyFunSpec

/**
  * Token Iterator Test
  * @author lawrence.daniels@gmail.com
  */
class TokenIteratorTest extends AnyFunSpec {

  describe(classOf[TokenIterator].getSimpleName) {

    it("supports low-level parsing of SELECT queries") {
      val query = "SELECT * FROM CompanyList WHERE Industry = 'Oil/Gas Transmission"
      val tok = TokenIterator(query)
      val results = tok.map(_.text).toList
      assert(results == List(
        "SELECT", "*", "FROM", "CompanyList", "WHERE", "Industry", "=", "Oil/Gas Transmission"
      ))
    }

    it("supports low-level parsing of INSERT-SELECT statements") {
      val query =
        """|INSERT INTO Test (Symbol, Sector, Industry, LastSale)
           |SELECT Symbol, Sector, Industry, LastSale
           |FROM CompanyList
           |WHERE Industry = 'Precious Metals'""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.map(_.text).toList
      assert(results == List(
        "INSERT", "INTO", "Test", "(", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale", ")",
        "SELECT", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale",
        "FROM", "CompanyList",
        "WHERE", "Industry", "=", "Precious Metals"
      ))
    }

    it("supports low-level parsing of INSERT-VALUES statements") {
      val query =
        """|INSERT INTO Test (Symbol, Sector, Industry, LastSale)
           |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
           |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.map(_.value).toList
      assert(results == List(
        "INSERT", "INTO", "Test", "(", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale", ")",
        "VALUES", "(", "ACU", ",", "Capital Goods", ",", "Industrial Machinery/Components", ",", 29d, ")",
        "VALUES", "(", "EMX", ",", "Basic Industries", ",", "Precious Metals", ",", 0.828, ")"
      ))
    }

  }

}

