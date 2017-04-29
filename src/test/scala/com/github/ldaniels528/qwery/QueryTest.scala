package com.github.ldaniels528.qwery

import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

/**
  * Query Test
  * @author lawrence.daniels@gmail.com
  */
class QueryTest extends FunSpec {
  private val connected = false

  describe("QueryCriteria") {

    it("should extract filtered results from a CVS file") {
      val languageParser = new QueryCompiler()
      val query = languageParser(
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)

      val results = query.execute().toSeq
      val tabular = new Tabular()
      tabular.transform(results.toIterable) foreach(info(_))

      assert(results == Stream(
        Map("" -> "", "Sector" -> "Public Utilities", "Name" -> "Cheniere Energy Partners LP Holdings, LLC", "ADR TSO" -> "n/a", "Industry" -> "Oil/Gas Transmission", "," -> ",", "Symbol" -> "CQH", "IPOyear" -> "n/a", "LastSale" -> "25.68", "Summary Quote" -> "http://www.nasdaq.com/symbol/cqh", "MarketCap" -> "5950056000"),
        Map("" -> "", "Sector" -> "Public Utilities", "Name" -> "Cheniere Energy Partners, LP", "ADR TSO" -> "n/a", "Industry" -> "Oil/Gas Transmission", "," -> ",", "Symbol" -> "CQP", "IPOyear" -> "n/a", "LastSale" -> "31.75", "Summary Quote" -> "http://www.nasdaq.com/symbol/cqp", "MarketCap" -> "10725987819"),
        Map("" -> "", "Sector" -> "Public Utilities", "Name" -> "Cheniere Energy, Inc.", "ADR TSO" -> "n/a", "Industry" -> "Oil/Gas Transmission", "," -> ",", "Symbol" -> "LNG", "IPOyear" -> "n/a", "LastSale" -> "45.35", "Summary Quote" -> "http://www.nasdaq.com/symbol/lng", "MarketCap" -> "10786934946.1"),
        Map("" -> "", "Sector" -> "Public Utilities", "Name" -> "Gas Natural Inc.", "ADR TSO" -> "n/a", "Industry" -> "Oil/Gas Transmission", "," -> ",", "Symbol" -> "EGAS", "IPOyear" -> "n/a", "LastSale" -> "12.5", "Summary Quote" -> "http://www.nasdaq.com/symbol/egas", "MarketCap" -> "131496600")
      ))
    }

    it("should extract filtered results from a URL") {
      if (connected) {
        val languageParser = new QueryCompiler()
        val query = languageParser(
          """
            |SELECT * FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
            |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)

        val tabular = new Tabular()
        tabular.transform(query.execute()) foreach(info(_))
      }
    }

  }

}
