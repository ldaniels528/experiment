package com.qwery.database.models

import org.scalatest.funspec.AnyFunSpec

import java.util.Date

/**
 * Key Values Test Suite
 */
class KeyValuesTest extends AnyFunSpec {

  describe(classOf[KeyValues].getName) {

    it("it should filter key-value pairs") {
      val kvp0 = KeyValues("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55, "lastSaleTime" -> new Date())
      val kvp1 = kvp0.filter { case (name, _) => name == "symbol" }
      assert(kvp1 == KeyValues("symbol" -> "AMD"))
    }

  }

}
