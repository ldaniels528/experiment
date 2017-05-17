package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.formats.{CSVFormatter, JSONFormatter, TextFormatter}
import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

import scala.io.Source

/**
  * Text Formatter Test
  * @author lawrence.daniels@gmail.com
  */
class TextFormatterTest extends FunSpec {
  val headers = List("Symbol", "Name", "LastSale", "MarketCap", "ADR TSO", "IPOyear", "Sector", "Industry", "Summary Quote")
  val tabular = new Tabular()

  describe("TextFormatter") {

    it("should be capable of parsing CSV text") {
      val csvFormatter = CSVFormatter(headers = headers)
      val results = Source.fromFile("./companylist.csv").getLines().take(5) map { line =>
        csvFormatter.fromText(line)
      }
      tabular.transform(results) foreach (info(_))
    }

    it("should be capable of determining the delimiter via auto-detection") {
      val lines = Source.fromFile("./companylist.csv").getLines()
      TextFormatter.autodetectDelimiter(lines) foreach { case (_, results) =>
        tabular.transform(results) foreach (info(_))
      }
    }

    it("should be capable of transforming CSV to JSON") {
      val csvFormatter = CSVFormatter(headers = headers)
      val jsonFormatter = JSONFormatter()
      val results = Source.fromFile("./companylist.csv").getLines().take(5).toSeq flatMap { line =>
        jsonFormatter.toText(csvFormatter.fromText(line))
      }
      results foreach (info(_))
    }

  }

}
