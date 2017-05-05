package com.github.ldaniels528.qwery

import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

/**
  * Query Test
  * @author lawrence.daniels@gmail.com
  */
class QueryTest extends FunSpec {
  private val connected = false

  describe("Query") {

    it("should extract filtered results from a CVS file") {
      val compiler = new QweryCompiler()
      val query = compiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)

      val results = query.execute().toSeq
      val tabular = new Tabular()
      tabular.transform(results.toIterator) foreach (info(_))

      assert(results == Seq(
        List("Symbol" -> "CQH", "Name" -> "Cheniere Energy Partners LP Holdings, LLC", "Sector" -> "Public Utilities",
          "Industry" -> "Oil/Gas Transmission", "LastSale" -> "25.68", "MarketCap" -> "5950056000"),
        List("Symbol" -> "CQP", "Name" -> "Cheniere Energy Partners, LP", "Sector" -> "Public Utilities",
          "Industry" -> "Oil/Gas Transmission", "LastSale" -> "31.75", "MarketCap" -> "10725987819"),
        List("Symbol" -> "LNG", "Name" -> "Cheniere Energy, Inc.", "Sector" -> "Public Utilities",
          "Industry" -> "Oil/Gas Transmission", "LastSale" -> "45.35", "MarketCap" -> "10786934946.1"),
        List("Symbol" -> "EGAS", "Name" -> "Gas Natural Inc.", "Sector" -> "Public Utilities",
          "Industry" -> "Oil/Gas Transmission", "LastSale" -> "12.5", "MarketCap" -> "131496600")
      ))
    }

    it("should extract filtered results from a URL") {
      if (connected) {
        val compiler = new QweryCompiler()
        val query = compiler(
          """
            |SELECT * FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
            |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)

        val tabular = new Tabular()
        tabular.transform(query.execute()) foreach (info(_))
      }
    }

  }

}
