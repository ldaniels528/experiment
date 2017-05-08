package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.util.StringHelper._
import org.scalatest.FunSpec

/**
  * String Helper Tests
  * @author lawrence.daniels@gmail.com
  */
class StringHelperTest extends FunSpec {

  describe("StringHelper") {

    it("should parse delimited text with quotes") {
      val text = """"Symbol","Name","LastSale","MarketCap","ADR TSO","IPOyear","Sector","Industry","Summary Quote","""
      info(s"text: $text")
      info(s"delimited text: ${text.delimitedSplit(',')}")
      assert(text.delimitedSplit(',') ==
        List("Symbol", "Name", "LastSale", "MarketCap", "ADR TSO", "IPOyear", "Sector", "Industry", "Summary Quote"))
    }

  }

}
