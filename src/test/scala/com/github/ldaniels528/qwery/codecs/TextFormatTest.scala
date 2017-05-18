package com.github.ldaniels528.qwery.codecs

import com.github.ldaniels528.tabular.Tabular
import org.scalatest.FunSpec

import scala.io.Source

/**
  * Text Format Test
  * @author lawrence.daniels@gmail.com
  */
class TextFormatTest extends FunSpec {
  val headers = List("Symbol", "Name", "LastSale", "MarketCap", "ADR TSO", "IPOyear", "Sector", "Industry", "Summary Quote")
  val tabular = new Tabular()

  describe("TextFormat") {

    it("should be capable of parsing CSV text") {
      val csvFormat = CSVFormat(headers = headers)
      val results = Source.fromFile("./companylist.csv").getLines().take(5) flatMap { line =>
        csvFormat.decode(line).toOption
      }
      tabular.transform(results) foreach (info(_))
    }

    it("should be capable of determining the delimiter via auto-detection") {
      val lines = Source.fromFile("./companylist.csv").getLines()
      TextFormat.autodetectDelimiter(lines) foreach { case (_, results) =>
        tabular.transform(results) foreach (info(_))
      }
    }

    it("should be capable of transforming CSV to JSON") {
      val csvFormat = CSVFormat(headers = headers)
      val jsonFormat = JSONFormat()
      val results = Source.fromFile("./companylist.csv").getLines().take(5).toSeq flatMap { line =>
        csvFormat.decode(line).map(jsonFormat.encode).toOption
      }
      results foreach (info(_))
    }

  }

}
