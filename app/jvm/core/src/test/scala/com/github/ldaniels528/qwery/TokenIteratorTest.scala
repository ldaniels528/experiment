package com.github.ldaniels528.qwery

import org.scalatest.FunSpec

/**
  * Token Iterator Test
  * @author lawrence.daniels@gmail.com
  */
class TokenIteratorTest extends FunSpec {

  describe("TokenIterator") {

    it("supports low-level parsing of SELECT queries") {
      val query =
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.toList.map(_.text)
      assert(results == List(
        "SELECT", "*", "FROM", "./companylist.csv",
        "WHERE", "Industry", "=", "Oil/Gas Transmission"
      ))
    }

    it("supports low-level parsing of INSERT-SELECT statements") {
      val query =
        """
          |INSERT INTO './test2.csv' (Symbol, Sector, Industry, LastSale)
          |SELECT Symbol, Sector, Industry, LastSale
          |FROM './companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.toList.map(_.text)
      assert(results == List(
        "INSERT", "INTO", "./test2.csv", "(", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale", ")",
        "SELECT", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale",
        "FROM", "./companylist.csv",
        "WHERE", "Industry", "=", "Precious Metals"
      ))
    }

    it("supports low-level parsing of INSERT-VALUES statements") {
      val query =
        """
          |INSERT INTO './test3.csv' (Symbol, Sector, Industry, LastSale)
          |VALUES ('ACU', 'Capital Goods', 'Industrial Machinery/Components', 29)
          |VALUES ('EMX', 'Basic Industries', 'Precious Metals', 0.828)""".stripMargin
      val tok = TokenIterator(query)
      val results = tok.toList.map(_.text)
      assert(results == List(
        "INSERT", "INTO", "./test3.csv", "(", "Symbol", ",", "Sector", ",", "Industry", ",", "LastSale", ")",
        "VALUES", "(", "ACU", ",", "Capital Goods", ",", "Industrial Machinery/Components", ",", "29", ")",
        "VALUES", "(", "EMX", ",", "Basic Industries", ",", "Precious Metals", ",", "0.828", ")"
      ))
    }

  }

}
