package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.RootScope
import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

import scala.util.Properties

/**
  * Query Test
  * @author lawrence.daniels@gmail.com
  */
class QueryTest extends FunSpec {
  private val tabular = new Tabular()

  describe("Query") {

    it("should extract filtered results from a CVS file") {
      val compiler = new QweryCompiler()
      val query = compiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = "Consumer Specialties""".stripMargin)

      val globalScope = new RootScope()
      val results = query.execute(globalScope).toSeq
      tabular.transform(results.toIterator) foreach (info(_))

      assert(results == Stream(
        List("Symbol" -> "BGI", "Name" -> "Birks Group Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.4401", "MarketCap" -> "25865464.7281"),
        List("Symbol" -> "DGSE", "Name" -> "DGSE Companies, Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.64", "MarketCap" -> "44125234.84")
      ))
    }

    it("should copy filtered results from one source to another") {
      val compiler = new QweryCompiler()
      val query = compiler(
        """
          |INSERT INTO './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Sector = 'Basic Industries'""".stripMargin)

      val globalScope = new RootScope()
      val results = query.execute(globalScope)
      assert(results == Stream(Seq(("ROWS_INSERTED", 44))))
    }

      it("should extract filtered results from a URL") {
      val connected = Properties.envOrNone("QWERY_WEB").nonEmpty
      if (connected) {
        val compiler = new QweryCompiler()
        val query = compiler(
          """
            |SELECT * FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
            |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)

        val globalScope = new RootScope()
        val results = query.execute(globalScope).toSeq
        tabular.transform(results.toIterator) foreach (info(_))

        assert(results == Stream(
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
    }

  }

}
