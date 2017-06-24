package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.ops.Row
import com.github.ldaniels528.qwery.util.JSONSupportTest.Sample
import org.scalatest.FunSpec

/**
  * JSON Support Test
  * @author lawrence.daniels@gmail.com
  */
class JSONSupportTest extends FunSpec {
  val jsonSupport = new JSONSupport {}

  describe("JSONSupport") {

    it("should parse a JSON string into a case class") {
      val sample = jsonSupport.parseJsonAs[Sample]("""{ "a" : 5, "b" : 5.6, "c" : "Hello World" }""")
      assert(sample == Sample(a = 5, b = 5.6, c = "Hello World"))
    }

    it("should parse a JSON string into a Row object") {
      val jsonString = """{ "symbol" : "AAPL", "price": 156.77, "data": "2017-06-24" }"""
      assert(jsonSupport.parseRow(jsonString) ==
        Row(columns = Seq("symbol" -> "AAPL", "price" -> 156.77, "data" -> "2017-06-24"))
      )
    }

    it("should parse a JSON string into an unfiltered Row object") {
      val jsonString =
        """
          |{
          | "data" : [
          |   { "symbol" : "AAPL", "price": 156.77, "date": "2017-06-24" },
          |   { "symbol" : "AMD", "price": 4.21, "date": "2017-06-24" },
          |   { "symbol" : "AMZN", "price": 596.88, "date": "2017-06-24" }
          | ]
          |}""".stripMargin
      assert(jsonSupport.parseRows(jsonString, jsonPath = Nil) == Seq(Seq(
        "data.symbol" -> "AAPL", "data.price" -> 156.77, "data.date" -> "2017-06-24",
        "data.symbol" -> "AMD", "data.price" -> 4.21, "data.date" -> "2017-06-24",
        "data.symbol" -> "AMZN", "data.price" -> 596.88, "data.date" -> "2017-06-24"): Row
      ))
    }

    it("should parse a JSON string into filtered Row objects") {
      val jsonString =
        """
          |{
          | "data" : [
          |   { "symbol" : "AAPL", "price": 156.77, "date": "2017-06-24" },
          |   { "symbol" : "AMD", "price": 4.21, "date": "2017-06-24" },
          |   { "symbol" : "AMZN", "price": 596.88, "date": "2017-06-24" }
          | ]
          |}""".stripMargin
      assert(jsonSupport.parseRows(jsonString, jsonPath = "data" :: Nil) == Seq(
        Seq("symbol" -> "AAPL", "price" -> 156.77, "date" -> "2017-06-24"): Row,
        Seq("symbol" -> "AMD", "price" -> 4.21, "date" -> "2017-06-24"): Row,
        Seq("symbol" -> "AMZN", "price" -> 596.88, "date" -> "2017-06-24"): Row
      ))
    }

  }

}

/**
  * JSON Support Test Companion
  * @author lawrence.daniels@gmail.com
  */
object JSONSupportTest {

  case class Sample(a: Int, b: Double, c: String)

}
