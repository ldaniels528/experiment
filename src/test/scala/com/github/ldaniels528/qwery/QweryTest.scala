package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.RootScope
import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

import scala.util.Properties

/**
  * Qwery Integration Test Suite
  * @author lawrence.daniels@gmail.com
  */
class QweryTest extends FunSpec {
  private val tabular = new Tabular()

  describe("Qwery") {

    it("should support describing the layout of a file") {
      val query = QweryCompiler("DESCRIBE './companylist.csv'")
      val results = query.execute(RootScope()).toSeq
      assert(results == Vector(
        List("COLUMN" -> "Symbol", "TYPE" -> "String", "SAMPLE" -> "ABE"),
        List("COLUMN" -> "Name", "TYPE" -> "String", "SAMPLE" -> "Aberdeen Emerging Markets Smaller Company Opportunities Fund I"),
        List("COLUMN" -> "LastSale", "TYPE" -> "String", "SAMPLE" -> "13.63"),
        List("COLUMN" -> "MarketCap", "TYPE" -> "String", "SAMPLE" -> "131446834.05"),
        List("COLUMN" -> "ADR TSO", "TYPE" -> "String", "SAMPLE" -> "n/a"),
        List("COLUMN" -> "IPOyear", "TYPE" -> "String", "SAMPLE" -> "n/a"),
        List("COLUMN" -> "Sector", "TYPE" -> "String", "SAMPLE" -> "n/a"),
        List("COLUMN" -> "Industry", "TYPE" -> "String", "SAMPLE" -> "n/a"),
        List("COLUMN" -> "Summary Quote", "TYPE" -> "String", "SAMPLE" -> "http://www.nasdaq.com/symbol/abe")
      ))
    }

    it("should support creating named aliases") {
      val query = QweryCompiler("SELECT 1234 AS number")
      val results = query.execute(RootScope()).toSeq
      assert(results == List(List("number" -> 1234.0)))
    }

    it("should support CASTing values from one type to another") {
      val query = QweryCompiler("SELECT CAST('1234' AS Double) AS number")
      val results = query.execute(RootScope()).toSeq
      assert(results == List(List("number" -> 1234.0)))
    }

    it("should support extracting filtered results from a file") {
      val query = QweryCompiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = 'Consumer Specialties'""".stripMargin)

      val results = query.execute(RootScope()).toSeq
      assert(results == Stream(
        List("Symbol" -> "BGI", "Name" -> "Birks Group Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.4401", "MarketCap" -> "25865464.7281"),
        List("Symbol" -> "DGSE", "Name" -> "DGSE Companies, Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.64", "MarketCap" -> "44125234.84")
      ))
    }

    it("should support extracting filtered results from a file for all (*) fields") {
      val query = QweryCompiler(
        """
          |SELECT * FROM './companylist.csv'
          |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
      val results = query.execute(RootScope()).toSeq
      assert(results ==
        Stream(List("Symbol" -> "CQH", "Name" -> "Cheniere Energy Partners LP Holdings, LLC", "LastSale" -> "25.68",
          "MarketCap" -> "5950056000", "ADR TSO" -> "n/a", "IPOyear" -> "n/a", "Sector" -> "Public Utilities",
          "Industry" -> "Oil/Gas Transmission", "Summary Quote" -> "http://www.nasdaq.com/symbol/cqh"),
          List("Symbol" -> "CQP", "Name" -> "Cheniere Energy Partners, LP", "LastSale" -> "31.75", "MarketCap" -> "10725987819",
            "ADR TSO" -> "n/a", "IPOyear" -> "n/a", "Sector" -> "Public Utilities", "Industry" -> "Oil/Gas Transmission",
            "Summary Quote" -> "http://www.nasdaq.com/symbol/cqp"),
          List("Symbol" -> "LNG", "Name" -> "Cheniere Energy, Inc.", "LastSale" -> "45.35", "MarketCap" -> "10786934946.1",
            "ADR TSO" -> "n/a", "IPOyear" -> "n/a", "Sector" -> "Public Utilities", "Industry" -> "Oil/Gas Transmission",
            "Summary Quote" -> "http://www.nasdaq.com/symbol/lng"),
          List("Symbol" -> "EGAS", "Name" -> "Gas Natural Inc.", "LastSale" -> "12.5", "MarketCap" -> "131496600", "ADR TSO" -> "n/a",
            "IPOyear" -> "n/a", "Sector" -> "Public Utilities", "Industry" -> "Oil/Gas Transmission",
            "Summary Quote" -> "http://www.nasdaq.com/symbol/egas")))
    }

    it("should support limiting results via 'TOP n'") {
      val query = QweryCompiler(
        """
          |SELECT TOP 1 Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = 'Consumer Specialties'""".stripMargin)

      val results = query.execute(RootScope()).toSeq
      assert(results == Stream(
        List("Symbol" -> "BGI", "Name" -> "Birks Group Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.4401", "MarketCap" -> "25865464.7281")
      ))
    }

    it("should support limiting results via 'LIMIT n'") {
      val query = QweryCompiler(
        """
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = 'Consumer Specialties'
          |LIMIT 1""".stripMargin)

      val results = query.execute(RootScope()).toSeq
      assert(results == Stream(
        List("Symbol" -> "BGI", "Name" -> "Birks Group Inc.", "Sector" -> "Consumer Services",
          "Industry" -> "Consumer Specialties", "LastSale" -> "1.4401", "MarketCap" -> "25865464.7281")
      ))
    }

    it("should support simple data aggregation") {
      val query = QweryCompiler(
        """
          |SELECT Sector, COUNT(*) AS Securities
          |FROM './companylist.csv'
          |GROUP BY Sector""".stripMargin)

      val results = query.execute(RootScope()).toSeq
      assert(results == List(
        List("Sector" -> "Consumer Durables", "Securities" -> 4L),
        List("Sector" -> "Consumer Non-Durables", "Securities" -> 13L),
        List("Sector" -> "Energy", "Securities" -> 30L),
        List("Sector" -> "Consumer Services", "Securities" -> 27L),
        List("Sector" -> "Transportation", "Securities" -> 1L),
        List("Sector" -> "n/a", "Securities" -> 120L),
        List("Sector" -> "Health Care", "Securities" -> 48L),
        List("Sector" -> "Basic Industries", "Securities" -> 44L),
        List("Sector" -> "Public Utilities", "Securities" -> 11L),
        List("Sector" -> "Capital Goods", "Securities" -> 24L),
        List("Sector" -> "Finance", "Securities" -> 12L),
        List("Sector" -> "Technology", "Securities" -> 20L),
        List("Sector" -> "Miscellaneous", "Securities" -> 5L)
      ))
    }

    it("should support aggregation functions like AVG, MIN, MAX and SUM") {
      val query = QweryCompiler(
        """
          |SELECT MIN(LastSale) AS min, MAX(LastSale) AS max, AVG(LastSale) AS avg, SUM(LastSale) AS total, COUNT(*) AS records
          |FROM './companylist.csv'""".stripMargin('|'))
      val results = query.execute(RootScope()).toSeq
      assert(results == List(
        List("min" -> 0.1213, "max" -> 4234.01, "avg" -> 23.68907242339833, "total" -> 8504.377, "records" -> 359)
      ))
    }

    it("should support aggregation functions like AVG, MIN, MAX and SUM via GROUP BY") {
      val query = QweryCompiler(
        """
          |SELECT MIN(LastSale) AS min, MAX(LastSale) AS max, AVG(LastSale) AS avg, SUM(LastSale) AS total, COUNT(*) AS records
          |FROM './companylist.csv'
          |GROUP BY Sector""".stripMargin('|'))
      val results = query.execute(RootScope()).toSeq
      tabular.transform(results.toIterator) foreach (info(_))
      /*
      assert(results == List(
        List("min" -> 0.1213, "max" -> 4234.01, "avg" -> 23.68907242339833, "total" -> 8504.377, "records" -> 359)
      ))*/
    }

    it("should write filtered results from one source (CSV) to another (CSV)") {
      val query = QweryCompiler(
        """
          |INSERT OVERWRITE './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Industry = 'Precious Metals'""".stripMargin)
      val results = query.execute(RootScope()).toSeq
      assert(results == Stream(Seq(("ROWS_INSERTED", 34))))
    }

    it("should overwrite/append filtered results from one source (CSV) to another (CSV)") {
      val queries = Seq(
        QweryCompiler(
          """
            |INSERT OVERWRITE './test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
            |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
            |FROM './companylist.csv'
            |WHERE Industry = 'Precious Metals'""".stripMargin),
        QweryCompiler(
          """
            |INSERT INTO './test2.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
            |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
            |FROM './companylist.csv'
            |WHERE Industry = 'Mining & Quarrying of Nonmetallic Minerals (No Fuels)'""".stripMargin)
      )
      val results = queries.map(_.execute(RootScope()))
      assert(results == Seq(
        Seq(Seq(("ROWS_INSERTED", 34))),
        Seq(Seq(("ROWS_INSERTED", 5)))
      ))
    }

    it("should write filtered results from one source (CSV) to another (JSON)") {
      val query = QweryCompiler(
        """
          |INSERT OVERWRITE './test3.json' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
          |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
          |FROM './companylist.csv'
          |WHERE Sector = 'Basic Industries'""".stripMargin)
      val results = query.execute(RootScope()).toSeq
      assert(results == Stream(Seq(("ROWS_INSERTED", 44))))
    }

    it("should extract filtered results from a URL") {
      val connected = Properties.envOrNone("QWERY_WEB").map(_.toLowerCase()).contains("true")
      if (connected) {
        val query = QweryCompiler(
          """
            |SELECT TOP 4 * FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
            |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)
        val results = query.execute(RootScope()).toSeq
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
