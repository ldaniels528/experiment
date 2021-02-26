package com.qwery.language

import com.qwery.language.SQLDecompiler.implicits._
import org.scalatest.funspec.AnyFunSpec

/**
  * SQL Decompiler Test Suite
  */
class SQLDecompilerTest extends AnyFunSpec {

  describe(SQLDecompiler.getClass.getSimpleName) {

    it("should decompile SQL models") {
      val sql =
        """|create view tickers as
           |select
           |   symbol as ticker,
           |   exchange as market,
           |   lastSale,
           |   round(lastSale, 1) as roundedLastSale,
           |   lastSaleTime
           |from securities
           |order by lastSale desc
           |limit 5
           |""".stripMargin
      val expected = SQLLanguageParser.parse(sql)
      val actual = SQLLanguageParser.parse(expected.toSQL)
      assert(expected == actual)
    }

  }

}
